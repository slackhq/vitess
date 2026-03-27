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

package logic

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtorc/config"
)

func TestCheckRecoveryCooldown_Disabled(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	// With duration=0 (disabled), should always return false
	config.SetRecoveryCooldownDuration(0)
	defer config.SetRecoveryCooldownDuration(0)

	active, err := checkRecoveryCooldown(context.Background(), "ks", "0")
	require.NoError(t, err)
	require.False(t, active)
}

func TestCheckRecoveryCooldown_NoMarker(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	config.SetRecoveryCooldownDuration(10 * time.Minute)
	defer config.SetRecoveryCooldownDuration(0)

	active, err := checkRecoveryCooldown(context.Background(), "ks", "0")
	require.NoError(t, err)
	require.False(t, active)
}

func TestCheckRecoveryCooldown_Active(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	config.SetRecoveryCooldownDuration(10 * time.Minute)
	defer config.SetRecoveryCooldownDuration(0)

	// Write a marker with a recent timestamp
	writeRecoveryCooldown("ks", "0", "DeadPrimary", "RecoverDeadPrimary")

	active, err := checkRecoveryCooldown(context.Background(), "ks", "0")
	require.NoError(t, err)
	require.True(t, active)
}

func TestCheckRecoveryCooldown_Expired(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	config.SetRecoveryCooldownDuration(1 * time.Second)
	defer config.SetRecoveryCooldownDuration(0)

	// Write a marker with a timestamp in the past
	marker := recoveryCooldownMarker{
		Timestamp:    time.Now().Add(-5 * time.Second),
		AnalysisCode: "DeadPrimary",
		RecoveryName: "RecoverDeadPrimary",
		InstanceID:   "test-host",
	}
	data, err := json.Marshal(marker)
	require.NoError(t, err)

	err = ts.UpsertMetadata(context.Background(), recoveryCooldownKey("ks", "0"), string(data))
	require.NoError(t, err)

	active, err := checkRecoveryCooldown(context.Background(), "ks", "0")
	require.NoError(t, err)
	require.False(t, active)
}

func TestCheckRecoveryCooldown_CorruptedMarker(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	config.SetRecoveryCooldownDuration(10 * time.Minute)
	defer config.SetRecoveryCooldownDuration(0)

	// Write garbage data as the marker
	err := ts.UpsertMetadata(context.Background(), recoveryCooldownKey("ks", "0"), "not-valid-json")
	require.NoError(t, err)

	// Should return false (non-fatal) for corrupted markers
	active, err := checkRecoveryCooldown(context.Background(), "ks", "0")
	require.NoError(t, err)
	require.False(t, active)
}

func TestWriteRecoveryCooldown(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	config.SetRecoveryCooldownDuration(10 * time.Minute)
	defer config.SetRecoveryCooldownDuration(0)

	writeRecoveryCooldown("ks", "0", "DeadPrimary", "RecoverDeadPrimary")

	// Read back the marker and verify
	val, err := ts.GetSingleMetadata(context.Background(), recoveryCooldownKey("ks", "0"))
	require.NoError(t, err)
	require.NotEmpty(t, val)

	var marker recoveryCooldownMarker
	err = json.Unmarshal([]byte(val), &marker)
	require.NoError(t, err)
	require.Equal(t, "DeadPrimary", marker.AnalysisCode)
	require.Equal(t, "RecoverDeadPrimary", marker.RecoveryName)
	require.NotEmpty(t, marker.InstanceID)
	require.WithinDuration(t, time.Now(), marker.Timestamp, 5*time.Second)
}

func TestWriteRecoveryCooldown_Disabled(t *testing.T) {
	oldTs := ts
	defer func() { ts = oldTs }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts = memorytopo.NewServer(ctx, "zone1")

	config.SetRecoveryCooldownDuration(0)

	writeRecoveryCooldown("ks", "0", "DeadPrimary", "RecoverDeadPrimary")

	// No marker should have been written
	val, err := ts.GetSingleMetadata(context.Background(), recoveryCooldownKey("ks", "0"))
	require.NoError(t, err)
	require.Empty(t, val)
}
