/*
Copyright 2023 The Vitess Authors.

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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
package mysqlctl

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestGetIncrementalFromPosGTIDSet(t *testing.T) {
	tcases := []struct {
		incrementalFromPos string
		gtidSet            string
		expctError         bool
	}{
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
		{
			"MySQL56/invalid",
			"",
			true,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.incrementalFromPos, func(t *testing.T) {
			gtidSet, err := getIncrementalFromPosGTIDSet(tcase.incrementalFromPos)
			if tcase.expctError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.gtidSet, gtidSet.String())
			}
		})
	}
}

func TestShouldDrainForBackupBuiltIn(t *testing.T) {
	be := &BuiltinBackupEngine{}

	assert.True(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "auto"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
	assert.False(t, be.ShouldDrainForBackup(&tabletmanagerdatapb.BackupRequest{IncrementalFromPos: "MySQL56/99ca8ed4-399c-11ee-861b-0a43f95f28a3:1-197"}))
}

// TestBackupFiles exercises the full backupFiles method, which handles file
// discovery, chunking decisions, concurrent dispatch, and MANIFEST writing.
// It creates a directory with a small file (below chunk threshold → whole-file
// backup) and a large file (above threshold → chunked backup), then verifies
// the resulting backup data and manifest. A restore roundtrip subtest verifies
// that the chunked data can be restored correctly.
func TestBackupFiles(t *testing.T) {
	origCompress := backupStorageCompress
	backupStorageCompress = false
	defer func() { backupStorageCompress = origCompress }()

	origThreshold := backupFileChunkThreshold
	origChunkSize := backupFileChunkSize
	backupFileChunkThreshold = 100
	backupFileChunkSize = 30
	defer func() {
		backupFileChunkThreshold = origThreshold
		backupFileChunkSize = origChunkSize
	}()

	// Set up directory structure that findFilesToBackup can scan.
	tmpDir := t.TempDir()
	innodbDataDir := filepath.Join(tmpDir, "innodb_data")
	innodbLogDir := filepath.Join(tmpDir, "innodb_log")
	dataDir := filepath.Join(tmpDir, "data")
	require.NoError(t, os.MkdirAll(innodbDataDir, 0755))
	require.NoError(t, os.MkdirAll(innodbLogDir, 0755))
	require.NoError(t, os.MkdirAll(dataDir, 0755))

	t.Logf("innodbDataDir: %s", innodbDataDir)

	// Small file (50 bytes, below 100-byte threshold) → whole-file backup.
	smallData := make([]byte, 50)
	for i := range smallData {
		smallData[i] = byte(i % 256)
	}
	require.NoError(t, os.WriteFile(filepath.Join(innodbDataDir, "small.ibd"), smallData, 0644))

	// Large file (150 bytes, above 100-byte threshold) → 5 chunks of 30 bytes.
	largeData := make([]byte, 150)
	for i := range largeData {
		largeData[i] = byte((i*7 + 13) % 256)
	}
	require.NoError(t, os.WriteFile(filepath.Join(innodbDataDir, "large.ibd"), largeData, 0644))

	// Set up file backup storage.
	backupRoot := filepath.Join(tmpDir, "backup_storage")
	require.NoError(t, os.MkdirAll(backupRoot, 0755))
	origRoot := filebackupstorage.FileBackupStorageRoot
	filebackupstorage.FileBackupStorageRoot = backupRoot
	t.Cleanup(func() { filebackupstorage.FileBackupStorageRoot = origRoot })

	ctx := context.Background()
	fbs := backupstorage.BackupStorageMap["file"]
	bh, err := fbs.StartBackup(ctx, "test_ks/test_shard", "test_backup")
	require.NoError(t, err)

	cnf := &Mycnf{
		InnodbDataHomeDir:     innodbDataDir,
		InnodbLogGroupHomeDir: innodbLogDir,
		DataDir:               dataDir,
	}
	be := &BuiltinBackupEngine{}
	params := BackupParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       stats.NoStats(),
		Concurrency: 4,
	}

	err = be.backupFiles(ctx, params, bh,
		replication.Position{}, replication.Position{}, replication.Position{},
		"", nil, "test-uuid", "8.0.0", nil,
	)
	require.NoError(t, err)

	// Read and parse the MANIFEST.
	backupDir := filepath.Join(backupRoot, "test_ks/test_shard", "test_backup")
	manifestData, err := os.ReadFile(filepath.Join(backupDir, backupManifestFileName))
	require.NoError(t, err)

	var bm builtinBackupManifest
	require.NoError(t, json.Unmarshal(manifestData, &bm))

	// findFilesToBackup scans InnodbDataHomeDir; os.ReadDir returns entries
	// alphabetically: large.ibd (index 0), small.ibd (index 1).
	require.Len(t, bm.FileEntries, 2)

	var smallFE, largeFE *FileEntry
	for i := range bm.FileEntries {
		switch bm.FileEntries[i].Name {
		case "large.ibd":
			largeFE = &bm.FileEntries[i]
		case "small.ibd":
			smallFE = &bm.FileEntries[i]
		}
	}
	require.NotNil(t, smallFE, "should find small.ibd in manifest")
	require.NotNil(t, largeFE, "should find large.ibd in manifest")

	// --- Small file: whole-file backup, no chunks ---
	assert.Empty(t, smallFE.Chunks, "small file should not be chunked")
	assert.NotEmpty(t, smallFE.Hash, "small file hash should be set")

	// Storage name for the whole file at index 1 is "1".
	gotSmall, err := os.ReadFile(filepath.Join(backupDir, "1"))
	require.NoError(t, err)
	assert.Equal(t, smallData, gotSmall)

	h := crc32.NewIEEE()
	h.Write(gotSmall)
	assert.Equal(t, hex.EncodeToString(h.Sum(nil)), smallFE.Hash)

	// --- Large file: chunked into 5 pieces ---
	require.Len(t, largeFE.Chunks, 5, "150 bytes / 30-byte chunks = 5 chunks")
	assert.Empty(t, largeFE.Hash, "chunked file should not have a top-level hash")

	for j, chunk := range largeFE.Chunks {
		assert.Equal(t, fmt.Sprintf("0-%d", j), chunk.StorageName)

		gotChunk, err := os.ReadFile(filepath.Join(backupDir, chunk.StorageName))
		require.NoError(t, err)
		expected := largeData[chunk.Offset : chunk.Offset+chunk.Size]
		assert.Equal(t, expected, gotChunk, "chunk %d data mismatch", j)

		h := crc32.NewIEEE()
		h.Write(gotChunk)
		assert.Equal(t, hex.EncodeToString(h.Sum(nil)), chunk.Hash, "chunk %d hash mismatch", j)
	}

	// --- Restore roundtrip for the chunked file ---
	t.Run("restore chunked file roundtrip", func(t *testing.T) {
		backups, err := fbs.ListBackups(ctx, "test_ks/test_shard")
		require.NoError(t, err)
		require.NotEmpty(t, backups)

		var readBh backupstorage.BackupHandle
		for _, b := range backups {
			if b.Name() == "test_backup" {
				readBh = b
				break
			}
		}
		require.NotNil(t, readBh, "should find the test_backup handle")

		restoreDir := filepath.Join(tmpDir, "restore_data")
		require.NoError(t, os.MkdirAll(restoreDir, 0755))

		destPath := filepath.Join(restoreDir, "large.ibd")
		dest, err := os.Create(destPath)
		require.NoError(t, err)
		require.NoError(t, dest.Truncate(int64(len(largeData))))

		restoreParams := RestoreParams{
			Cnf:    cnf,
			Logger: logutil.NewMemoryLogger(),
			Stats:  stats.NoStats(),
		}

		for j := range largeFE.Chunks {
			chunk := &largeFE.Chunks[j]
			err := be.restoreFileChunk(ctx, restoreParams, readBh, chunk, bm, dest, nil)
			require.NoError(t, err, "chunk %d restore should succeed", j)
		}
		require.NoError(t, dest.Close())

		restoredData, err := os.ReadFile(destPath)
		require.NoError(t, err)
		assert.Equal(t, largeData, restoredData,
			"restored data should match original (got %d bytes, want %d)", len(restoredData), len(largeData))
	})

	// --- Full restoreFiles roundtrip: verify file sizes match originals ---
	t.Run("restoreFiles preserves file sizes", func(t *testing.T) {
		backups, err := fbs.ListBackups(ctx, "test_ks/test_shard")
		require.NoError(t, err)
		require.NotEmpty(t, backups)

		var readBh backupstorage.BackupHandle
		for _, b := range backups {
			if b.Name() == "test_backup" {
				readBh = b
				break
			}
		}
		require.NotNil(t, readBh, "should find the test_backup handle")

		// Record original file sizes (apparent and disk blocks).
		type fileInfo struct {
			apparentSize int64
			blocks       int64
		}
		origFiles := map[string]fileInfo{}
		for _, name := range []string{"small.ibd", "large.ibd"} {
			fi, err := os.Stat(filepath.Join(innodbDataDir, name))
			require.NoError(t, err)
			stat := fi.Sys().(*syscall.Stat_t)
			origFiles[name] = fileInfo{
				apparentSize: fi.Size(),
				blocks:       stat.Blocks,
			}
			t.Logf("original %s: apparent=%d blocks=%d", name, fi.Size(), stat.Blocks)
		}

		// Restore into a fresh directory tree.
		restoreDir := filepath.Join(tmpDir, "restore_full")
		restoreInnodbDataDir := filepath.Join(restoreDir, "innodb_data")
		restoreInnodbLogDir := filepath.Join(restoreDir, "innodb_log")
		restoreDataDir := filepath.Join(restoreDir, "data")
		for _, d := range []string{restoreInnodbDataDir, restoreInnodbLogDir, restoreDataDir} {
			require.NoError(t, os.MkdirAll(d, 0755))
		}

		restoreCnf := &Mycnf{
			InnodbDataHomeDir:     restoreInnodbDataDir,
			InnodbLogGroupHomeDir: restoreInnodbLogDir,
			DataDir:               restoreDataDir,
		}
		restoreParams := RestoreParams{
			Cnf:         restoreCnf,
			Logger:      logutil.NewMemoryLogger(),
			Stats:       stats.NoStats(),
			Concurrency: 4,
		}

		_, err = be.restoreFiles(ctx, restoreParams, readBh, bm)
		require.NoError(t, err)

		// Compare restored file sizes with originals.
		for name, orig := range origFiles {
			fi, err := os.Stat(filepath.Join(restoreInnodbDataDir, name))
			require.NoError(t, err, "restored %s should exist", name)
			stat := fi.Sys().(*syscall.Stat_t)
			t.Logf("restored %s: apparent=%d blocks=%d", name, fi.Size(), stat.Blocks)

			assert.Equal(t, orig.apparentSize, fi.Size(),
				"restored %s apparent size should match original", name)
			assert.Equal(t, orig.blocks, stat.Blocks,
				"restored %s disk blocks should match original", name)
		}
	})

	// --- Concurrent restore roundtrip (mirrors production restoreFiles pattern) ---
	t.Run("restore chunked file concurrently", func(t *testing.T) {
		backups, err := fbs.ListBackups(ctx, "test_ks/test_shard")
		require.NoError(t, err)
		require.NotEmpty(t, backups)

		var readBh backupstorage.BackupHandle
		for _, b := range backups {
			if b.Name() == "test_backup" {
				readBh = b
				break
			}
		}
		require.NotNil(t, readBh, "should find the test_backup handle")

		restoreDir := filepath.Join(tmpDir, "restore_concurrent")
		require.NoError(t, os.MkdirAll(restoreDir, 0755))

		destPath := filepath.Join(restoreDir, "large.ibd")
		dest, err := os.Create(destPath)
		require.NoError(t, err)
		require.NoError(t, dest.Truncate(int64(len(largeData))))

		restoreParams := RestoreParams{
			Cnf:    cnf,
			Logger: logutil.NewMemoryLogger(),
			Stats:  stats.NoStats(),
		}

		var wg sync.WaitGroup
		errs := make([]error, len(largeFE.Chunks))
		for j := range largeFE.Chunks {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				chunk := &largeFE.Chunks[idx]
				errs[idx] = be.restoreFileChunk(ctx, restoreParams, readBh, chunk, bm, dest, nil)
			}(j)
		}
		wg.Wait()

		for j, e := range errs {
			require.NoError(t, e, "chunk %d concurrent restore should succeed", j)
		}
		require.NoError(t, dest.Close())

		restoredData, err := os.ReadFile(destPath)
		require.NoError(t, err)
		assert.Equal(t, largeData, restoredData,
			"concurrently restored data should match original (got %d bytes, want %d)", len(restoredData), len(largeData))
	})
}

// TestRestoreFileSizePreserved verifies that a full backup+restore roundtrip
// with compression enabled produces restored files with the exact same apparent
// size and disk block count as the originals.
func TestRestoreFileSizePreserved(t *testing.T) {
	origCompress := backupStorageCompress
	backupStorageCompress = true
	defer func() { backupStorageCompress = origCompress }()

	origEngine := CompressionEngineName
	CompressionEngineName = Lz4Compressor
	defer func() { CompressionEngineName = origEngine }()

	origThreshold := backupFileChunkThreshold
	origChunkSize := backupFileChunkSize
	backupFileChunkThreshold = 100
	backupFileChunkSize = 30
	defer func() {
		backupFileChunkThreshold = origThreshold
		backupFileChunkSize = origChunkSize
	}()

	tmpDir := t.TempDir()
	innodbDataDir := filepath.Join(tmpDir, "innodb_data")
	innodbLogDir := filepath.Join(tmpDir, "innodb_log")
	dataDir := filepath.Join(tmpDir, "data")
	for _, d := range []string{innodbDataDir, innodbLogDir, dataDir} {
		require.NoError(t, os.MkdirAll(d, 0755))
	}

	// Small file (50 bytes, below 100-byte threshold) → whole-file backup.
	smallData := make([]byte, 50)
	for i := range smallData {
		smallData[i] = byte(i % 256)
	}
	require.NoError(t, os.WriteFile(filepath.Join(innodbDataDir, "small.ibd"), smallData, 0644))

	// Large file (150 bytes, above 100-byte threshold) → 5 chunks of 30 bytes.
	largeData := make([]byte, 150)
	for i := range largeData {
		largeData[i] = byte((i*7 + 13) % 256)
	}
	require.NoError(t, os.WriteFile(filepath.Join(innodbDataDir, "large.ibd"), largeData, 0644))

	// Record original file sizes.
	type fileInfo struct {
		apparentSize int64
		blocks       int64
	}
	origFiles := map[string]fileInfo{}
	for _, name := range []string{"small.ibd", "large.ibd"} {
		fi, err := os.Stat(filepath.Join(innodbDataDir, name))
		require.NoError(t, err)
		stat := fi.Sys().(*syscall.Stat_t)
		origFiles[name] = fileInfo{apparentSize: fi.Size(), blocks: stat.Blocks}
		t.Logf("original %s: apparent=%d blocks=%d", name, fi.Size(), stat.Blocks)
	}

	// Set up backup storage.
	backupRoot := filepath.Join(tmpDir, "backup_storage")
	require.NoError(t, os.MkdirAll(backupRoot, 0755))
	origRoot := filebackupstorage.FileBackupStorageRoot
	filebackupstorage.FileBackupStorageRoot = backupRoot
	t.Cleanup(func() { filebackupstorage.FileBackupStorageRoot = origRoot })

	ctx := context.Background()
	fbs := backupstorage.BackupStorageMap["file"]

	cnf := &Mycnf{
		InnodbDataHomeDir:     innodbDataDir,
		InnodbLogGroupHomeDir: innodbLogDir,
		DataDir:               dataDir,
	}

	// Backup.
	bh, err := fbs.StartBackup(ctx, "test_ks/test_shard", "size_test")
	require.NoError(t, err)

	be := &BuiltinBackupEngine{}
	err = be.backupFiles(ctx, BackupParams{
		Cnf:         cnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       stats.NoStats(),
		Concurrency: 4,
	}, bh,
		replication.Position{}, replication.Position{}, replication.Position{},
		"", nil, "test-uuid", "8.0.0", nil,
	)
	require.NoError(t, err)

	// Parse manifest.
	backupDir := filepath.Join(backupRoot, "test_ks/test_shard", "size_test")
	manifestData, err := os.ReadFile(filepath.Join(backupDir, backupManifestFileName))
	require.NoError(t, err)

	var bm builtinBackupManifest
	require.NoError(t, json.Unmarshal(manifestData, &bm))

	// Restore.
	backups, err := fbs.ListBackups(ctx, "test_ks/test_shard")
	require.NoError(t, err)
	var readBh backupstorage.BackupHandle
	for _, b := range backups {
		if b.Name() == "size_test" {
			readBh = b
			break
		}
	}
	require.NotNil(t, readBh)

	restoreDir := filepath.Join(tmpDir, "restore")
	restoreInnodbDataDir := filepath.Join(restoreDir, "innodb_data")
	for _, d := range []string{
		restoreInnodbDataDir,
		filepath.Join(restoreDir, "innodb_log"),
		filepath.Join(restoreDir, "data"),
	} {
		require.NoError(t, os.MkdirAll(d, 0755))
	}

	restoreCnf := &Mycnf{
		InnodbDataHomeDir:     restoreInnodbDataDir,
		InnodbLogGroupHomeDir: filepath.Join(restoreDir, "innodb_log"),
		DataDir:               filepath.Join(restoreDir, "data"),
	}
	_, err = be.restoreFiles(ctx, RestoreParams{
		Cnf:         restoreCnf,
		Logger:      logutil.NewMemoryLogger(),
		Stats:       stats.NoStats(),
		Concurrency: 4,
	}, readBh, bm)
	require.NoError(t, err)

	// Compare restored file sizes with originals.
	for name, orig := range origFiles {
		fi, err := os.Stat(filepath.Join(restoreInnodbDataDir, name))
		require.NoError(t, err, "restored %s should exist", name)
		stat := fi.Sys().(*syscall.Stat_t)
		t.Logf("restored %s: apparent=%d blocks=%d", name, fi.Size(), stat.Blocks)

		assert.Equal(t, orig.apparentSize, fi.Size(),
			"restored %s apparent size should match original", name)
		assert.Equal(t, orig.blocks, stat.Blocks,
			"restored %s disk blocks should match original", name)
	}
}
