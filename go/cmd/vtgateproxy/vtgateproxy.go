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

package main

import (
	"fmt"
	"net/http"

	channelz "github.com/rantav/go-grpc-channelz"
	"google.golang.org/grpc/channelz/service"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/stats/prometheusbackend"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgateproxy"
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterHandoffFlags()
}

func main() {
	defer exit.Recover()

	servenv.ParseFlags("vtgateproxy")
	servenv.Init()

	prometheusbackend.Init("vtgateproxy")

	servenv.OnRun(func() {
		// channelz is served over gRPC, so we bind it to the generic servenv server; the http
		// handler queries that server locally for observability.
		service.RegisterChannelzServiceToServer(servenv.GRPCServer)

		// Register the channelz handler to /channelz/ (note trailing / which is required).
		http.Handle("/", channelz.CreateHandler("/", fmt.Sprintf(":%d", servenv.GRPCPort())))

		vtgateproxy.Init()
	})

	servenv.OnClose(func() {
	})
	servenv.RunDefault()
}
