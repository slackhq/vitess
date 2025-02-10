/*
Copyright 2025 The Vitess Authors.

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
	"errors"
	"net"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/handoff"
	"vitess.io/vitess/go/vt/log"
)

var (
	// handoffPath specifies the filesystem path where handoff sockets are to be created,
	// if zero-downtime handoff is enabled.
	//
	// To expose this flag, call RegisterHandoffFlags before ParseFlags.
	rootHandoffPath string
)

func RegisterHandoffFlags() {
	OnParse(func(fs *pflag.FlagSet) {
		fs.StringVar(&rootHandoffPath, "handoff_path", rootHandoffPath, "Root path to enable zero-downtime handoff sockets.")
	})
}

// HandoffOrListen implements optional support for zero-downtime socket handoff.
//
// If enabled by configuring a root path for the handoff, this first attempts to
// take over the socket from a running process, otherwise this just calls the regular
// net.Listen. Once it takes over the socket or creates a new one, it will also start
// listening for requests to hand off the socket to future runs.
func HandoffOrListen(serviceName, protocol, address string) (net.Listener, error) {

	// If there is no path to handoff, then just pass through to the core net.listen.
	if rootHandoffPath == "" {
		return net.Listen(protocol, address)
	}

	handoffPath := rootHandoffPath + serviceName

	// Request socket from an already running process, or start a new
	// listener to serve requests on.
	log.Infof("handoff: requesting handoff socket from %s", handoffPath)
	listener, err := handoff.Request(handoffPath)
	if err == nil {
		log.Infof("handoff: received handoff from %s", handoffPath)
	} else {
		if errors.Is(err, handoff.ErrNoHandoff) {
			log.Infof("handoff: no handoff, listening on %s %s", protocol, address)
			listener, err = net.Listen(protocol, address)
		} else {
			log.Exitf("handoff: fatal error: %v", err)
		}
	}

	// Advertise unix domain socket for handoff by future processes.
	go func() {
		err := handoff.Listen(handoffPath, listener)
		if err != nil {
			log.Errorf("Handoff failed: %v", err)
			return
		}

		log.Infof("handed off socket %s %s", protocol, address)
	}()

	return listener, err
}
