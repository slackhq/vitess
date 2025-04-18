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

package main

import (
	"fmt"

	"vitess.io/vitess/go/cmd/vtaclcheck/cli"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/logutil"
)

func init() {
	logger := logutil.NewConsoleLogger()
	writer := logutil.NewLoggerWriter(logger)
	cli.Main.SetOut(writer)
	cli.Main.SetErr(writer)
}

func main() {
	defer exit.RecoverAll()

	if err := cli.Main.Execute(); err != nil {
		fmt.Printf("ERROR: %s\n", err)
		exit.Return(1)
	}
}
