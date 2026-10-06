/*
Copyright 2019 The Vitess Authors.

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

package servenv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/orca"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

func TestEmpty(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	if len(interceptors.Build()) > 0 {
		t.Fatalf("expected empty builder to report as empty")
	}
}

func TestSingleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake := &FakeInterceptor{}

	interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
}

func TestDoubleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake1 := &FakeInterceptor{name: "ettan"}
	fake2 := &FakeInterceptor{name: "tvaon"}

	interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
	interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
}

func TestOrcaRecorder(t *testing.T) {
	recorder := orca.NewServerMetricsRecorder()

	recorder.SetCPUUtilization(0.25)
	recorder.SetMemoryUtilization(0.5)

	snap := recorder.ServerMetrics()

	if snap.CPUUtilization != 0.25 {
		t.Errorf("expected cpu 0.25, got %v", snap.CPUUtilization)
	}
	if snap.MemUtilization != 0.5 {
		t.Errorf("expected memory 0.5, got %v", snap.MemUtilization)
	}
}

func TestReportedOrca(t *testing.T) {
	// Set the port to enable gRPC server.
	withTempVar(&gRPCPort, getFreePort())
	withTempVar(&gRPCEnableOrcaMetrics, true)
	withTempVar(&GRPCServerMetricsRecorder, nil)

	createGRPCServer()
	if GRPCServerMetricsRecorder == nil {
		t.Errorf("GRPCServerMetricsRecorder should be initialized when gRPCEnableOrcaMetrics is false")
	}

	serveGRPC()
	serverMetrics := GRPCServerMetricsRecorder.ServerMetrics()
	cpuUsage := serverMetrics.CPUUtilization
	if cpuUsage < 0 {
		t.Errorf("CPU Utilization is not set %.2f", cpuUsage)
	}
	t.Logf("CPU Utilization is %.2f", cpuUsage)

	memUsage := serverMetrics.MemUtilization
	if memUsage < 0 {
		t.Errorf("Mem Utilization is not set %.2f", memUsage)
	}
	t.Logf("Memory utilization is %.2f", memUsage)
}

func TestOrcaQPSKeepsReportingMessagesOfOpenVStream(t *testing.T) {
	client := vtgateservicepb.NewVitessClient(startOrcaQPSTestServer(t))
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	stream, err := client.VStream(ctx, &vtgatepb.VStreamRequest{})
	require.NoError(t, err)
	streamStart := time.Now()
	go drainStream(stream.Recv)

	// Check a window well after the stream opened, so the rate is sustained.
	require.Eventually(t, func() bool {
		return time.Since(streamStart) > 5*orcaUpdateInterval && GRPCServerMetricsRecorder.ServerMetrics().QPS > 100
	}, 30*time.Second, 10*time.Millisecond, "expected ORCA QPS to reflect messages sent on an already-open VStream")

	// Never can run its condition after returning, so it must not read the global.
	recorder := GRPCServerMetricsRecorder
	assert.Never(t, func() bool {
		return recorder.ServerMetrics().QPS == 0
	}, 5*orcaUpdateInterval, 10*time.Millisecond, "expected ORCA QPS to stay nonzero while the VStream keeps sending")
}

type orcaQPSTestVitessServer struct {
	vtgateservicepb.UnimplementedVitessServer
}

func (orcaQPSTestVitessServer) VStream(_ *vtgatepb.VStreamRequest, stream vtgateservicepb.Vitess_VStreamServer) error {
	response := &vtgatepb.VStreamResponse{Events: []*binlogdatapb.VEvent{{
		Type: binlogdatapb.VEventType_HEARTBEAT,
	}}}
	for {
		if err := stream.Send(response); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(time.Millisecond):
		}
	}
}

func startOrcaQPSTestServer(t *testing.T) *grpc.ClientConn {
	t.Helper()

	port := getFreePort()
	t.Cleanup(withTempVar(&gRPCPort, port))
	t.Cleanup(withTempVar(&gRPCBindAddress, "127.0.0.1"))
	t.Cleanup(withTempVar(&gRPCEnableOrcaMetrics, true))
	t.Cleanup(withTempVar(&orcaUpdateInterval, 100*time.Millisecond))
	t.Cleanup(withTempVar(&GRPCServerMetricsRecorder, nil))
	t.Cleanup(withTempVar(&GRPCServer, (*grpc.Server)(nil)))
	orcaEgressMessages.Store(0)

	createGRPCServer()
	vtgateservicepb.RegisterVitessServer(GRPCServer, orcaQPSTestVitessServer{})
	stopOrcaUpdater := serveGRPC()
	t.Cleanup(stopOrcaUpdater)
	t.Cleanup(GRPCServer.Stop)

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func drainStream[T any](recv func() (T, error)) error {
	for {
		if _, err := recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func getFreePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(fmt.Sprintf("could not get free port: %v", err))
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func withTempVar[T any](set *T, temp T) (restore func()) {
	original := *set
	*set = temp
	return func() {
		*set = original
	}
}

type FakeInterceptor struct {
	name       string
	streamSeen any
	unarySeen  any
}

func (fake *FakeInterceptor) StreamServerInterceptor(value any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fake.streamSeen = value
	return handler(value, stream)
}

func (fake *FakeInterceptor) UnaryServerInterceptor(ctx context.Context, value any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	fake.unarySeen = value
	return handler(ctx, value)
}
