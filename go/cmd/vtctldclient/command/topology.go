/*
Copyright 2022 The Vitess Authors.

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

package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	// GetTopologyPath makes a GetTopologyPath gRPC call to a vtctld.
	GetTopologyPath = &cobra.Command{
		Use:                   "GetTopologyPath <path>",
		Short:                 "Gets the value associated with the particular path (key) in the topology server.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetTopologyPath,
	}

	// SetVtorcEmergencyReparent enables/disables the use of EmergencyReparentShard in VTOrc recoveries for a given keyspace or keyspace/shard.
	SetVtorcEmergencyReparent = &cobra.Command{
		Use:                   "SetVtorcEmergencyReparent [--enable|-e] [--disable|-d] <keyspace> <shard>",
		Short:                 "Enable/disables the use of EmergencyReparentShard in VTOrc recoveries for a given keyspace or keyspace/shard.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"setvtorcemergencyreparent"},
		Args:                  cobra.RangeArgs(1, 2),
		RunE:                  commandSetVtorcEmergencyReparent,
	}
)

func commandGetTopologyPath(cmd *cobra.Command, args []string) error {
	path := cmd.Flags().Arg(0)

	cli.FinishedParsing(cmd)

	resp, err := client.GetTopologyPath(commandCtx, &vtctldatapb.GetTopologyPathRequest{
		Path: path,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Cell)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var setVtorcEmergencyReparentOptions = struct {
	Disable bool
	Enable  bool
}{}

func commandSetVtorcEmergencyReparent(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	shard := cmd.Flags().Arg(1)
	keyspaceShard := topoproto.KeyspaceShardString(ks, shard)
	if !setVtorcEmergencyReparentOptions.Disable && !setVtorcEmergencyReparentOptions.Enable {
		return fmt.Errorf("SetVtorcEmergencyReparent(%v) error: must set --enable or --disable flag", keyspaceShard)
	}
	if setVtorcEmergencyReparentOptions.Disable && setVtorcEmergencyReparentOptions.Enable {
		return fmt.Errorf("SetVtorcEmergencyReparent(%v) error: --enable and --disable flags are mutually exclusive", keyspaceShard)
	}

	_, err := client.SetVtorcEmergencyReparent(commandCtx, &vtctldatapb.SetVtorcEmergencyReparentRequest{
		Keyspace: ks,
		Shard:    shard,
		Disable:  setVtorcEmergencyReparentOptions.Disable,
	})

	if err != nil {
		return fmt.Errorf("SetVtorcEmergencyReparent(%v) error: %w; please check the topo", keyspaceShard, err)
	}

	fmt.Printf("Successfully updated keyspace/shard %v.\n", keyspaceShard)

	return nil
}

func init() {
	Root.AddCommand(GetTopologyPath)

	Root.AddCommand(SetVtorcEmergencyReparent)
	SetVtorcEmergencyReparent.Flags().BoolVarP(&setVtorcEmergencyReparentOptions.Disable, "disable", "d", false, "Disable the use of EmergencyReparentShard in recoveries.")
	SetVtorcEmergencyReparent.Flags().BoolVarP(&setVtorcEmergencyReparentOptions.Enable, "enable", "e", false, "Enable the use of EmergencyReparentShard in recoveries.")
}
