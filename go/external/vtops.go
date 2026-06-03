package external

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

const vtopsSendSlackMessageCommand = "send-slack-message"

var (
	vtopsBinaryPath     string        = os.Getenv("VTOPS_PATH")
	vtopsExecTimeout    time.Duration = time.Second * 10
	vtopsMaxConcurrency int64         = 12

	statsVtopsInflightExecutions   *stats.GaugesWithSingleLabel
	statsVtopsMaxConcurrency       *stats.Gauge
	statsVtopsFailedExecutions     *stats.CountersWithMultiLabels
	statsVtopsSuccessfulExecutions *stats.CountersWithSingleLabel
)

func init() {
	// flags
	servenv.OnParse(func(fs *pflag.FlagSet) {
		fs.StringVar(&vtopsBinaryPath, "vtops-binary-path", vtopsBinaryPath,
			"path to the slack vtops binary")
		fs.DurationVar(&vtopsExecTimeout, "vtops-exec-timeout", vtopsExecTimeout,
			"execution timeout for the slack vtops binary")
		fs.Int64Var(&vtopsMaxConcurrency, "vtops-max-concurrency", vtopsMaxConcurrency,
			"max concurrency for executing the slack vtops binary")
	})

	// stats
	statsVtopsInflightExecutions = stats.NewGaugesWithSingleLabel("VtopsInflightExecutions",
		"number of vtops executions currently running", "type")
	statsVtopsMaxConcurrency = stats.NewGauge("VtopsMaxConcurrency",
		"maximum number of concurrent vtops executions")
	statsVtopsFailedExecutions = stats.NewCountersWithMultiLabels("VtopsFailedExecutions",
		"number of failures executing the slack vtops binary", []string{"type", "exit_code"})
	statsVtopsSuccessfulExecutions = stats.NewCountersWithSingleLabel("VtopsSuccessfulExecutions",
		"number of times the slack vtops binary was successfully executed", "type")
}

type outputHandler func(stderr, stdout *bytes.Buffer)

func logOutputHandler(stderr, stdout *bytes.Buffer) {
	if stdout.Len() > 0 {
		log.Infof("vtops stdout: %s", stdout.String())
	}
	if stderr.Len() > 0 {
		log.Errorf("vtops stderr: %s", stderr.String())
	}
}

func vtorcServiceName() string {
	return fmt.Sprintf("vtorc-%s-%s", os.Getenv("POOL"), os.Getenv("VITESS_ENVIRONMENT"))
}

type ExecVTOps struct {
	BinaryPath string
	Service    string
	Hostname   string

	// output handler
	outputHandler outputHandler

	// prevent too many concurrent execs
	sem *semaphore.Weighted
}

func NewExecVTOps() *ExecVTOps {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	statsVtopsMaxConcurrency.Set(vtopsMaxConcurrency)
	return &ExecVTOps{
		BinaryPath:    vtopsBinaryPath,
		Service:       vtorcServiceName(),
		Hostname:      hostname,
		outputHandler: logOutputHandler,
		sem:           semaphore.NewWeighted(vtopsMaxConcurrency),
	}
}

func (e *ExecVTOps) execCommand(name string, vtopsArgs ...string) error {
	if e.BinaryPath == "" {
		return nil
	}
	command := filepath.Base(e.BinaryPath)

	statsVtopsInflightExecutions.Add(name, 1)
	defer statsVtopsInflightExecutions.Add(name, -1)

	ctx, cancel := context.WithTimeout(context.Background(), vtopsExecTimeout)
	defer cancel()

	// wait here if too many concurrent execs
	if err := e.sem.Acquire(ctx, 1); err != nil {
		log.Errorf("Failed to acquire semaphore for %s command: %+v", command, err)
		statsVtopsFailedExecutions.Add([]string{name, ""}, 1)
		return err
	}
	defer e.sem.Release(1)

	log.Infof("Running %s command: %s %s", command, e.BinaryPath, strings.Join(vtopsArgs, " "))

	cmd := exec.CommandContext(ctx, e.BinaryPath, vtopsArgs...)
	if e.outputHandler != nil {
		var stderr, stdout bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Stdout = &stdout
		defer e.outputHandler(&stderr, &stdout)
	}

	if err := cmd.Run(); err != nil {
		log.Errorf("Error executing %q command: %+v", command, err)
		exitCode := 1
		var execErr *exec.ExitError
		if errors.As(err, &execErr) {
			exitCode = execErr.ExitCode()
		}
		statsVtopsFailedExecutions.Add([]string{name, strconv.Itoa(exitCode)}, 1)
		return err
	}

	statsVtopsSuccessfulExecutions.Add(name, 1)
	return nil
}

func (e *ExecVTOps) SendSlackMessage(message string, channel string) {
	if e.BinaryPath == "" {
		log.Warningf("No vtops binary path set, not sending message to slack channel %s", channel)
		return
	}

	message = fmt.Sprintf("[%s/%s] %s", e.Service, e.Hostname, message)
	go e.execCommand("SendSlackMessage", vtopsSendSlackMessageCommand, //nolint:errcheck
		"--channel", channel,
		"--message", message,
		"--sender", e.Service,
	)
}
