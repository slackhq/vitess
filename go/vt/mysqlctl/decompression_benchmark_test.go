//go:build unix

/*
Copyright 2026 The Vitess Authors.

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
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/vt/logutil"
)

const (
	decompressBenchmarkDataSize = 64 * 1024 * 1024
	decompressBenchmarkPageSize = 16 * 1024
	decompressBenchmarkReadSize = 2 * 1024 * 1024
)

// BenchmarkDecompressBuiltin measures restore-style decompression throughput
// of the builtin engines, for a single stream and for concurrent streams.
func BenchmarkDecompressBuiltin(b *testing.B) {
	data := decompressBenchmarkData()
	engines := []struct {
		name   string
		engine string
		level  int
	}{
		{"zstd-fastest", ZstdCompressor, 1}, // default --compression-level, 4 MiB window
		{"zstd-default", ZstdCompressor, 2}, // 8 MiB window
		{"pgzip", PgzipCompressor, 1},
	}

	for _, e := range engines {
		compressed := compressForBenchmark(b, e.engine, e.level, data)
		for _, streams := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/streams=%d", e.name, streams), func(b *testing.B) {
				b.SetBytes(int64(streams * len(data)))
				b.ReportAllocs()
				cpuStart := processCPUTime(b)
				b.ResetTimer()

				for range b.N {
					var g errgroup.Group
					for range streams {
						g.Go(func() error {
							return decompressForBenchmark(e.engine, compressed, len(data))
						})
					}
					require.NoError(b, g.Wait())
				}

				b.StopTimer()
				cpu := processCPUTime(b) - cpuStart
				b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
				b.ReportMetric(float64(len(data))/float64(len(compressed)), "compression-ratio")
			})
		}
	}
}

// decompressBenchmarkData returns deterministic, semi-compressible data. Each
// page starts with a quarter of random bytes, repeated once, followed by a copy
// of the first half of one of the previous 255 pages, so every copy is within
// the smallest (4 MiB) zstd window.
func decompressBenchmarkData() []byte {
	src := rand.NewChaCha8([32]byte{})
	rng := rand.New(src)
	data := make([]byte, decompressBenchmarkDataSize)
	quarter := decompressBenchmarkPageSize / 4

	for off := 0; off < len(data); off += decompressBenchmarkPageSize {
		page := data[off : off+decompressBenchmarkPageSize]
		_, _ = src.Read(page[:quarter])
		copy(page[quarter:2*quarter], page[:quarter])
		from := off - (1+rng.IntN(255))*decompressBenchmarkPageSize
		if from < 0 {
			from = off
		}
		copy(page[2*quarter:], data[from:from+2*quarter])
	}
	return data
}

// compressForBenchmark compresses data with the builtin compressor used by
// backups, at the given compression level.
func compressForBenchmark(b *testing.B, engine string, level int, data []byte) []byte {
	defer func(saved int) { compressionLevel = saved }(compressionLevel)
	compressionLevel = level

	var buf bytes.Buffer
	c, err := newBuiltinCompressor(engine, &buf, logutil.NewMemoryLogger())
	require.NoError(b, err)
	_, err = c.Write(data)
	require.NoError(b, err)
	require.NoError(b, c.Close())
	return buf.Bytes()
}

// decompressForBenchmark decompresses the way a restore does: through the
// builtin decompressor, reading into a 2 MiB buffer.
func decompressForBenchmark(engine string, compressed []byte, want int) error {
	d, err := newBuiltinDecompressor(engine, bytes.NewReader(compressed), logutil.NewMemoryLogger())
	if err != nil {
		return fmt.Errorf("failed to create %s decompressor: %w", engine, err)
	}

	buf := make([]byte, decompressBenchmarkReadSize)
	total := 0
	for {
		n, err := d.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Join(fmt.Errorf("failed to decompress %s: %w", engine, err), d.Close())
		}
	}

	if err := d.Close(); err != nil {
		return fmt.Errorf("failed to close %s decompressor: %w", engine, err)
	}
	if total != want {
		return fmt.Errorf("%s decompressed %d bytes, want %d", engine, total, want)
	}
	return nil
}

// processCPUTime returns the user and system CPU time used by this process.
func processCPUTime(b *testing.B) time.Duration {
	var ru syscall.Rusage
	require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &ru))
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
