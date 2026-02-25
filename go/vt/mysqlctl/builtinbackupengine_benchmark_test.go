/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
)

// benchRestoreEnv holds handles needed by the benchmark loop after setup.
type benchRestoreEnv struct {
	manifest builtinBackupManifest
	readBh   backupstorage.BackupHandle
}

// throttledReadCloser wraps an io.ReadCloser and limits read throughput using
// a token-bucket rate limiter.
type throttledReadCloser struct {
	rc      io.ReadCloser
	limiter *rate.Limiter
	ctx     context.Context
}

func (t *throttledReadCloser) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		burst := t.limiter.Burst()
		remaining := n
		for remaining > 0 {
			take := remaining
			if take > burst {
				take = burst
			}
			if wErr := t.limiter.WaitN(t.ctx, take); wErr != nil {
				return n, wErr
			}
			remaining -= take
		}
	}
	return n, err
}

func (t *throttledReadCloser) Close() error {
	return t.rc.Close()
}

// throttledBackupHandle wraps a BackupHandle and throttles each ReadFile stream
// to bytesPerSec to simulate limited per-stream bandwidth (e.g. S3).
type throttledBackupHandle struct {
	backupstorage.BackupHandle
	bytesPerSec int
}

func (h *throttledBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	rc, err := h.BackupHandle.ReadFile(ctx, filename)
	if err != nil {
		return nil, err
	}
	const burstSize = 1024 * 1024 // 1 MB
	limiter := rate.NewLimiter(rate.Limit(h.bytesPerSec), burstSize)
	return &throttledReadCloser{rc: rc, limiter: limiter, ctx: ctx}, nil
}

// benchTempDir creates a temporary directory under base and registers cleanup.
func benchTempDir(b *testing.B, base string) string {
	b.Helper()
	dir, err := os.MkdirTemp(base, "bench-*")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// shmAvailable returns true if /dev/shm exists and has at least requiredBytes
// of free space.
func shmAvailable(requiredBytes int64) bool {
	if _, err := os.Stat("/dev/shm"); err != nil {
		return false
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/dev/shm", &stat); err != nil {
		return false
	}
	available := int64(stat.Bavail) * int64(stat.Bsize)
	return available >= requiredBytes
}

// generateBenchmarkData creates a directory tree with a single pseudo-random
// 1 GB InnoDB data file and returns the corresponding Mycnf.
func generateBenchmarkData(b *testing.B, baseDir string, dataSize int64) *Mycnf {
	b.Helper()

	innodbDataDir := filepath.Join(baseDir, "innodb_data")
	innodbLogDir := filepath.Join(baseDir, "innodb_log")
	dataDir := filepath.Join(baseDir, "data")
	for _, d := range []string{innodbDataDir, innodbLogDir, dataDir} {
		require.NoError(b, os.MkdirAll(d, 0755))
	}

	testFile := filepath.Join(innodbDataDir, "test.ibd")
	f, err := os.Create(testFile)
	require.NoError(b, err)

	rng := rand.New(rand.NewSource(42))
	const bufSize = 1024 * 1024 // 1 MB
	buf := make([]byte, bufSize)
	remaining := dataSize
	for remaining > 0 {
		n := int64(bufSize)
		if n > remaining {
			n = remaining
		}
		rng.Read(buf[:n])
		_, err := f.Write(buf[:n])
		require.NoError(b, err)
		remaining -= n
	}
	require.NoError(b, f.Close())

	return &Mycnf{
		InnodbDataHomeDir:     innodbDataDir,
		InnodbLogGroupHomeDir: innodbLogDir,
		DataDir:               dataDir,
	}
}

// createBenchmarkBackup runs backupFiles with the given compression engine and
// chunking parameters against an already-existing data directory, and returns
// the parsed manifest and a read-only backup handle. Use engine "none" to
// disable compression entirely.
func createBenchmarkBackup(b *testing.B, baseDir string, cnf *Mycnf, engine string, chunkThreshold, chunkSize int64) *benchRestoreEnv {
	b.Helper()

	origCompress := backupStorageCompress
	origEngine := CompressionEngineName
	if engine == "none" {
		backupStorageCompress = false
	} else {
		backupStorageCompress = true
		CompressionEngineName = engine
	}
	b.Cleanup(func() {
		backupStorageCompress = origCompress
		CompressionEngineName = origEngine
	})

	origThreshold := backupFileChunkThreshold
	origChunkSize := backupFileChunkSize
	backupFileChunkThreshold = chunkThreshold
	backupFileChunkSize = chunkSize
	b.Cleanup(func() {
		backupFileChunkThreshold = origThreshold
		backupFileChunkSize = origChunkSize
	})

	backupRoot := benchTempDir(b, baseDir)
	origRoot := filebackupstorage.FileBackupStorageRoot
	filebackupstorage.FileBackupStorageRoot = backupRoot
	b.Cleanup(func() { filebackupstorage.FileBackupStorageRoot = origRoot })

	ctx := context.Background()
	fbs := backupstorage.BackupStorageMap["file"]

	bh, err := fbs.StartBackup(ctx, "bench_ks/bench_shard", "bench_backup")
	require.NoError(b, err)

	be := &BuiltinBackupEngine{}
	params := BackupParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       stats.NoStats(),
		Concurrency: 4,
	}

	err = be.backupFiles(ctx, params, bh,
		replication.Position{}, replication.Position{}, replication.Position{},
		"", nil, "bench-uuid", "8.0.0", nil,
	)
	require.NoError(b, err)

	backups, err := fbs.ListBackups(ctx, "bench_ks/bench_shard")
	require.NoError(b, err)
	require.NotEmpty(b, backups)

	var readBh backupstorage.BackupHandle
	for _, bk := range backups {
		if bk.Name() == "bench_backup" {
			readBh = bk
			break
		}
	}
	require.NotNil(b, readBh, "should find bench_backup handle")

	manifestPath := filepath.Join(backupRoot, "bench_ks/bench_shard", "bench_backup", backupManifestFileName)
	manifestData, err := os.ReadFile(manifestPath)
	require.NoError(b, err)

	var manifest builtinBackupManifest
	require.NoError(b, json.Unmarshal(manifestData, &manifest))

	return &benchRestoreEnv{
		manifest: manifest,
		readBh:   readBh,
	}
}

// BenchmarkChunkedRestore benchmarks the restore path with different compression
// engines, chunking configurations, and storage types against a 1GB synthetic file.
//
// Run all scenarios:
//
//	go test -v -bench=BenchmarkChunkedRestore -benchtime=3x -count=1 ./go/vt/mysqlctl/
//
// Run a single storage/engine combination:
//
//	go test -v -bench='BenchmarkChunkedRestore/1GB/disk/pgzip' -benchtime=3x ./go/vt/mysqlctl/
//
// Run a single scenario:
//
//	go test -v -bench='BenchmarkChunkedRestore/1GB/memory/lz4/4_chunks' -benchtime=3x ./go/vt/mysqlctl/
func BenchmarkChunkedRestore(b *testing.B) {
	const dataSize int64 = 1024 * 1024 * 1024 // 1 GB

	// The source data lives on disk; only backup and restore I/O are
	// placed on the selected storage type.
	cnf := generateBenchmarkData(b, b.TempDir(), dataSize)

	// Backup + restore need roughly 2x the data size on the target storage.
	const shmRequired = 2 * dataSize

	type storageType struct {
		label string
		// mkDir returns a temp directory on the target storage type.
		mkDir func(b *testing.B) string
		// wrapBh optionally wraps the BackupHandle (e.g. to add throttling).
		wrapBh func(bh backupstorage.BackupHandle) backupstorage.BackupHandle
	}

	storageTypes := []storageType{
		{
			label: "memory",
			mkDir: func(b *testing.B) string {
				b.Helper()
				if !shmAvailable(shmRequired) {
					b.Skip("/dev/shm is not available or does not have enough free space")
				}
				return benchTempDir(b, "/dev/shm")
			},
		},
		{
			label: "s3-simulated",
			mkDir: func(b *testing.B) string {
				b.Helper()
				if !shmAvailable(shmRequired) {
					b.Skip("/dev/shm is not available or does not have enough free space")
				}
				return benchTempDir(b, "/dev/shm")
			},
			wrapBh: func(bh backupstorage.BackupHandle) backupstorage.BackupHandle {
				return &throttledBackupHandle{BackupHandle: bh, bytesPerSec: 85 * 1024 * 1024}
			}, //
		},
		{
			label: "disk",
			mkDir: func(b *testing.B) string {
				b.Helper()
				return b.TempDir()
			},
		},
	}

	engines := []string{"none", PgzipCompressor, Lz4Compressor, ZstdCompressor}

	chunkScenarios := []struct {
		label          string
		chunkThreshold int64
		chunkSize      int64
		concurrency    int
	}{
		{
			label:          "no_chunking",
			chunkThreshold: 2 * 1024 * 1024 * 1024, // 2 GB, above file size
			chunkSize:      0,                      // irrelevant
			concurrency:    4,
		},
		{
			label:          "chunking_concurrency_4",
			chunkThreshold: 32 * 1024 * 1024, // 32 MB
			chunkSize:      64 * 1024 * 1024, // 64 MB → 16 chunks
			concurrency:    4,
		},
		{
			label:          "chunking_concurrency_16",
			chunkThreshold: 32 * 1024 * 1024, // 32 MB
			chunkSize:      64 * 1024 * 1024, // 64 MB → 16 chunks
			concurrency:    16,
		},
		{
			label:          "chunking_concurrency_32",
			chunkThreshold: 16 * 1024 * 1024, // 16 MB
			chunkSize:      32 * 1024 * 1024, // 32 MB → 32 chunks
			concurrency:    32,
		},
	}

	for _, st := range storageTypes {
		b.Run("1GB/"+st.label, func(b *testing.B) {
			baseDir := st.mkDir(b)
			b.Logf("baseDir: %s", baseDir)

			for _, engine := range engines {
				b.Run(engine, func(b *testing.B) {
					for _, sc := range chunkScenarios {
						b.Run(sc.label, func(b *testing.B) {
							env := createBenchmarkBackup(b, baseDir, cnf, engine, sc.chunkThreshold, sc.chunkSize)

							be := &BuiltinBackupEngine{}
							ctx := context.Background()

							b.SetBytes(dataSize)
							b.ResetTimer()

							for i := 0; i < b.N; i++ {
								restoreDir, err := os.MkdirTemp(baseDir, "bench-*")
								if err != nil {
									b.Fatalf("MkdirTemp failed: %v", err)
								}
								restoreCnf := &Mycnf{
									InnodbDataHomeDir:     filepath.Join(restoreDir, "innodb_data"),
									InnodbLogGroupHomeDir: filepath.Join(restoreDir, "innodb_log"),
									DataDir:               filepath.Join(restoreDir, "data"),
								}

								restoreParams := RestoreParams{
									Cnf:         restoreCnf,
									Logger:      logutil.NewMemoryLogger(),
									Stats:       stats.NoStats(),
									Concurrency: sc.concurrency,
								}

								bh := env.readBh
								if st.wrapBh != nil {
									bh = st.wrapBh(env.readBh)
								}
								_, err = be.restoreFiles(ctx, restoreParams, bh, env.manifest)
								if err != nil {
									b.Fatalf("restoreFiles failed: %v", err)
								}
								os.RemoveAll(restoreDir)
							}
						})
					}
				})
			}
		})
	}
}
