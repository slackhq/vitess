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

package grpccommon

import (
	"flag"
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/stats"
)

var (
	defaultMaxMessageSize = 16 * 1024 * 1024
	// MaxMessageSize is the maximum message size which the gRPC server will
	// accept. Larger messages will be rejected.
	// Note: We're using 16 MiB as default value because that's the default in MySQL
	MaxMessageSize = flag.Int("grpc_max_message_size", defaultMaxMessageSize, "Maximum allowed RPC message size. Larger messages will be rejected by gRPC with the error 'exceeding the max size'.")
	// These options override MaxMessageSize if > 0, allowing us to control the max
	// size sending independently from receiving.
	MaxMsgRecvSize = flag.Int("grpc_max_message_recv_size", 0, "Maximum allowed RPC message size when receiving. If 0, defaults to grpc_max_message_size.")
	MaxMsgSendSize = flag.Int("grpc_max_message_send_size", 0, "Maximum allowed RPC message size when sending. If 0, defaults to grpc_max_message_size.")

	// EnableTracing sets a flag to enable grpc client/server tracing.
	EnableTracing = flag.Bool("grpc_enable_tracing", false, "Enable GRPC tracing")

	// EnableGRPCPrometheus sets a flag to enable grpc client/server grpc monitoring.
	EnableGRPCPrometheus = flag.Bool("grpc_prometheus", false, "Enable gRPC monitoring with Prometheus")
)

var enableTracing sync.Once

// EnableTracingOpt enables grpc tracing if requested.
// It must be called before any grpc server or client is created but is safe
// to be called multiple times.
func EnableTracingOpt() {
	enableTracing.Do(func() {
		grpc.EnableTracing = *EnableTracing
	})
}

func MaxMessageRecvSize() int {
	if *MaxMsgRecvSize > 0 {
		return *MaxMsgRecvSize
	}
	return *MaxMessageSize
}

func MaxMessageSendSize() int {
	if *MaxMsgSendSize > 0 {
		return *MaxMsgSendSize
	}
	return *MaxMessageSize
}

func init() {
	stats.NewString("GrpcVersion").Set(grpc.Version)
}
