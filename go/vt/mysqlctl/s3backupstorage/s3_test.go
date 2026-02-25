package s3backupstorage

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type s3FakeClient struct {
	s3iface.S3API
	err   error
	delay time.Duration
}

func (sfc *s3FakeClient) PutObjectRequest(in *s3.PutObjectInput) (*request.Request, *s3.PutObjectOutput) {
	u, _ := url.Parse("http://localhost:1234")
	req := request.Request{
		HTTPRequest: &http.Request{ // without this we segfault \_(ツ)_/¯ (see https://github.com/aws/aws-sdk-go/blob/v1.28.8/aws/request/request_context.go#L13)
			Header: make(http.Header),
			URL:    u,
		},
		Retryer: client.DefaultRetryer{},
	}

	req.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = sfc.err
		if sfc.delay > 0 {
			time.Sleep(sfc.delay)
		}
	})

	return &req, &s3.PutObjectOutput{}
}

func TestAddFileError(t *testing.T) {
	bh := &S3BackupHandle{
		client: &s3FakeClient{err: errors.New("some error")},
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
		},
		readOnly: false,
	}

	wc, err := bh.AddFile(aws.BackgroundContext(), "somefile", 100000)
	require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

	err = wc.Close()
	require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.True(t, bh.HasErrors(), "AddFile() expected bh to record async error but did not")
}

func TestAddFileStats(t *testing.T) {
	fakeStats := stats.NewFakeStats()

	delay := 10 * time.Millisecond

	bh := &S3BackupHandle{
		client: &s3FakeClient{delay: delay},
		bs: &S3BackupStorage{
			params: backupstorage.Params{
				Logger: logutil.NewMemoryLogger(),
				Stats:  fakeStats,
			},
		},
		readOnly: false,
	}

	for i := 0; i < 4; i++ {
		wc, err := bh.AddFile(aws.BackgroundContext(), fmt.Sprintf("somefile-%d", i), 100000)
		require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
		assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

		n, err := wc.Write([]byte("here are some bytes"))
		require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

		err = wc.Close()
		require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)
	}

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.Equal(t, bh.HasErrors(), false, "AddFile() expected bh not to record async errors but did")

	require.Len(t, fakeStats.ScopeCalls, 4)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Len(t, scopedStats.TimedIncrementCalls, 1)
	require.GreaterOrEqual(t, scopedStats.TimedIncrementCalls[0], delay)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestAddFileErrorStats(t *testing.T) {
	fakeStats := stats.NewFakeStats()

	delay := 10 * time.Millisecond

	bh := &S3BackupHandle{
		client: &s3FakeClient{
			delay: delay,
			err:   errors.New("some error"),
		},
		bs: &S3BackupStorage{
			params: backupstorage.Params{
				Logger: logutil.NewMemoryLogger(),
				Stats:  fakeStats,
			},
		},
		readOnly: false,
	}

	wc, err := bh.AddFile(aws.BackgroundContext(), "somefile", 100000)
	require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

	err = wc.Close()
	require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.True(t, bh.HasErrors(), "AddFile() expected bh not to record async errors but did")

	require.Len(t, fakeStats.ScopeCalls, 1)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Len(t, scopedStats.TimedIncrementCalls, 1)
	require.GreaterOrEqual(t, scopedStats.TimedIncrementCalls[0], delay)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestNoSSE(t *testing.T) {
	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Nil(t, sseData.awsAlg, "awsAlg expected to be nil")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")
}

func TestSSEAws(t *testing.T) {
	sse = "aws:kms"
	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Equal(t, aws.String("aws:kms"), sseData.awsAlg, "awsAlg expected to be aws:kms")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")

	assert.Nil(t, sseData.awsAlg, "awsAlg expected to be nil")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestSSECustomerFileNotFound(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	err = os.Remove(tempFile.Name())
	require.NoErrorf(t, err, "Remove() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.Errorf(t, err, "init() expected to fail")
}

func TestSSECustomerFileBinaryKey(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoErrorf(t, err, "Read() expected to succeed")
	_, err = tempFile.Write(randomKey)
	require.NoErrorf(t, err, "Write() expected to succeed")
	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Nil(t, sseData.awsAlg, "awsAlg expected to be nil")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")

	assert.Nil(t, sseData.awsAlg, "awsAlg expected to be nil")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestSSECustomerFileBase64Key(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoErrorf(t, err, "Read() expected to succeed")

	base64Key := base64.StdEncoding.EncodeToString(randomKey[:])
	_, err = tempFile.WriteString(base64Key)
	require.NoErrorf(t, err, "WriteString() expected to succeed")
	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Nil(t, sseData.awsAlg, "awsAlg expected to be nil")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")

	assert.Nil(t, sseData.awsAlg, "awsAlg expected to be nil")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestNewS3Transport(t *testing.T) {
	s3 := newS3BackupStorage()

	// checking some of the values are present in the returned transport and match the http.DefaultTransport.
	assert.Equal(t, http.DefaultTransport.(*http.Transport).IdleConnTimeout, s3.transport.IdleConnTimeout)
	assert.Equal(t, http.DefaultTransport.(*http.Transport).MaxIdleConns, s3.transport.MaxIdleConns)
	assert.NotNil(t, s3.transport.DialContext)
	assert.NotNil(t, s3.transport.Proxy)
}

func TestWithParams(t *testing.T) {
	bases3 := newS3BackupStorage()
	s3 := bases3.WithParams(backupstorage.Params{}).(*S3BackupStorage)
	// checking some of the values are present in the returned transport and match the http.DefaultTransport.
	assert.Equal(t, http.DefaultTransport.(*http.Transport).IdleConnTimeout, s3.transport.IdleConnTimeout)
	assert.Equal(t, http.DefaultTransport.(*http.Transport).MaxIdleConns, s3.transport.MaxIdleConns)
	assert.NotNil(t, s3.transport.DialContext)
	assert.NotNil(t, s3.transport.Proxy)
}

func TestCalculateUploadPartSize(t *testing.T) {
	originalMinimum := minPartSize
	defer func() { minPartSize = originalMinimum }()

	tests := []struct {
		name            string
		filesize        int64
		minimumPartSize int64
		want            int64
		err             error
	}{
		{
			name:            "minimum - 10 MiB",
			filesize:        1024 * 1024 * 10, // 10 MiB
			minimumPartSize: 1024 * 1024 * 5,  // 5 MiB
			want:            1024 * 1024 * 5,  // 5 MiB,
			err:             nil,
		},
		{
			name:            "below minimum - 10 MiB",
			filesize:        1024 * 1024 * 10, // 10 MiB
			minimumPartSize: 1024 * 1024 * 8,  // 8 MiB
			want:            1024 * 1024 * 8,  // 8 MiB,
			err:             nil,
		},
		{
			name:            "above minimum - 1 TiB",
			filesize:        1024 * 1024 * 1024 * 1024, // 1 TiB
			minimumPartSize: 1024 * 1024 * 5,           // 5 MiB
			want:            109951163,                 // ~104 MiB
			err:             nil,
		},
		{
			name:            "below minimum - 1 TiB",
			filesize:        1024 * 1024 * 1024 * 1024, // 1 TiB
			minimumPartSize: 1024 * 1024 * 200,         // 200 MiB
			want:            1024 * 1024 * 200,         // 200 MiB
			err:             nil,
		},
		{
			name:            "below S3 limits - 5 MiB",
			filesize:        1024 * 1024 * 3, // 3 MiB
			minimumPartSize: 1024 * 1024 * 4, // 4 MiB
			want:            1024 * 1024 * 5, // 5 MiB - should always return the minimum
			err:             nil,
		},
		{
			name:            "above S3 limits - 5 GiB",
			filesize:        1024 * 1024 * 1024 * 1024, // 1 TiB
			minimumPartSize: 1024 * 1024 * 1024 * 6,    // 6 GiB
			want:            0,
			err:             ErrPartSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minPartSize = tt.minimumPartSize
			partSize, err := calculateUploadPartSize(tt.filesize)
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, tt.want, partSize)
		})
	}
}

func TestSequentialWriterAt_InOrder(t *testing.T) {
	sw := newSequentialWriterAt()

	// Write three sequential chunks in order.
	go func() {
		sw.WriteAt([]byte("hello"), 0)
		sw.WriteAt([]byte(" "), 5)
		sw.WriteAt([]byte("world"), 6)
		sw.CloseWriter(nil)
	}()

	got, err := io.ReadAll(sw.pr)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(got))
}

func TestSequentialWriterAt_OutOfOrder(t *testing.T) {
	sw := newSequentialWriterAt()

	// Write chunks out of order: chunk at offset 5 arrives before offset 0.
	go func() {
		// Second chunk arrives first.
		sw.WriteAt([]byte("world"), 5)
		// First chunk arrives — should trigger flush of both.
		sw.WriteAt([]byte("hello"), 0)
		sw.CloseWriter(nil)
	}()

	got, err := io.ReadAll(sw.pr)
	require.NoError(t, err)
	assert.Equal(t, "helloworld", string(got))
}

func TestSequentialWriterAt_ConcurrentWriters(t *testing.T) {
	sw := newSequentialWriterAt()
	chunkSize := 1024
	numChunks := 100
	expected := make([]byte, chunkSize*numChunks)

	// Fill expected with deterministic data.
	for i := range expected {
		expected[i] = byte(i % 256)
	}

	// Launch concurrent writers, each responsible for one chunk.
	var wg sync.WaitGroup
	wg.Add(numChunks)
	for i := 0; i < numChunks; i++ {
		go func(idx int) {
			defer wg.Done()
			off := int64(idx * chunkSize)
			data := expected[off : off+int64(chunkSize)]
			_, err := sw.WriteAt(data, off)
			if err != nil {
				return // consumer may have closed
			}
		}(i)
	}

	go func() {
		wg.Wait()
		sw.CloseWriter(nil)
	}()

	got, err := io.ReadAll(sw.pr)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestSequentialWriterAt_DownloadError(t *testing.T) {
	sw := newSequentialWriterAt()
	downloadErr := errors.New("S3 download failed")

	go func() {
		sw.WriteAt([]byte("partial"), 0)
		sw.CloseWriter(downloadErr)
	}()

	got, err := io.ReadAll(sw.pr)
	// We should get the partial data that was flushed before the error.
	assert.Equal(t, "partial", string(got))
	assert.ErrorIs(t, err, downloadErr)
}

func TestSequentialWriterAt_ConsumerCloses(t *testing.T) {
	sw := newSequentialWriterAt()

	// Write first chunk so there's data, then close the reader.
	done := make(chan error, 1)
	go func() {
		_, err := sw.WriteAt([]byte("first"), 0)
		if err != nil {
			done <- err
			return
		}
		// By now the reader is closed, so the next WriteAt should fail.
		_, err = sw.WriteAt([]byte("second"), 5)
		done <- err
	}()

	// Read the first chunk, then close the reader.
	buf := make([]byte, 5)
	_, err := io.ReadFull(sw.pr, buf)
	require.NoError(t, err)
	assert.Equal(t, "first", string(buf))
	sw.pr.Close()

	// The writer goroutine should get an error.
	err = <-done
	assert.Error(t, err)
}

func TestSequentialWriterAt_BufferCopy(t *testing.T) {
	// Verify that WriteAt copies the data, so reusing the input buffer
	// doesn't corrupt the output (simulating Download Manager buffer reuse).
	sw := newSequentialWriterAt()

	go func() {
		buf := make([]byte, 5)
		copy(buf, "aaaaa")
		sw.WriteAt(buf, 0)
		// Reuse the same buffer with different data.
		copy(buf, "bbbbb")
		sw.WriteAt(buf, 5)
		sw.CloseWriter(nil)
	}()

	got, err := io.ReadAll(sw.pr)
	require.NoError(t, err)
	assert.Equal(t, "aaaaabbbbb", string(got))
}

func TestSequentialWriterAt_LargeData(t *testing.T) {
	sw := newSequentialWriterAt()
	partSize := 5 * 1024 * 1024 // 5MB, matching S3 default part size
	numParts := 3
	totalSize := partSize * numParts

	expected := make([]byte, totalSize)
	rand.Read(expected)

	go func() {
		// Write parts in reverse order.
		for i := numParts - 1; i >= 0; i-- {
			off := int64(i * partSize)
			sw.WriteAt(expected[off:off+int64(partSize)], off)
		}
		sw.CloseWriter(nil)
	}()

	got, err := io.ReadAll(sw.pr)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(expected, got), "large data mismatch")
}
