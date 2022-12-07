package vtgate

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"vitess.io/vitess/go/streamlog"
)

var standardFormatter streamlog.LogFormatter
var hostname string

// NewMurronLogFormatter returns a formatter for the query log that can be used to write to the rsyslog murron socket
func NewMurronLogFormatter(formatter streamlog.LogFormatter) streamlog.LogFormatter {
	standardFormatter = formatter

	// The compiler does not like directly assigning to hostname below, so we use a temporary theHostname variable here ¯\_(ツ)_/¯
	theHostname, err := os.Hostname()
	if err != nil {
		theHostname = "unknown-bedrock-vtgate"
	}

	hostname = theHostname

	return queryLogMurronFormatter
}

type murronMockIOWriter struct {
	data []byte
}

func (m *murronMockIOWriter) Write(msg []byte) (int, error) {
	m.data = msg

	return len(msg), nil
}

func queryLogMurronFormatter(out io.Writer, params url.Values, message interface{}) error {
	fakeWriter := &murronMockIOWriter{}

	err := standardFormatter(fakeWriter, params, message)
	if err != nil {
		return err
	}

	murronMessage := []byte(fmt.Sprintf("%s vtgate_query_log %s %s\n", time.Now().Format(time.RFC3339), hostname, string(fakeWriter.data)))

	_, err = out.Write(murronMessage)
	if err != nil {
		return err
	}

	return nil
}
