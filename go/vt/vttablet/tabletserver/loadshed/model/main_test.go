/*
Copyright 2026 The Vitess Authors.

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
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEaseCount(t *testing.T) {
	assert.Equal(t, 99, easeCount(100, 3))
	assert.Equal(t, 97, easeCount(100, 2))
	assert.Equal(t, 1, easeCount(2, 2))
	assert.Equal(t, 1, easeCount(1, 2))
}

func TestControllerFractionalEasingAccumulatesExtraStep(t *testing.T) {
	c, err := newController(controllerConfig{
		easingMode:            easingFractional,
		easingLogBase:         2,
		fractionalStrength:    0.5,
		fractionalCreditDecay: 0.5,
		recovery:              recoveryNone,
		memoryHorizon:         16,
	})
	require.NoError(t, err)

	c.count = 100
	c.observe(true)
	assert.Equal(t, 99, c.count)
	assert.InDelta(t, 0.962, c.fractionalCredit, 0.001)

	c.observe(true)
	assert.Equal(t, 97, c.count)
	assert.InDelta(t, 0.923, c.fractionalCredit, 0.001)
}

func TestControllerFractionalEasingZeroStrengthMatchesBaseThree(t *testing.T) {
	c, err := newController(controllerConfig{
		easingMode:            easingFractional,
		easingLogBase:         2,
		fractionalStrength:    0,
		fractionalCreditDecay: 0.5,
		recovery:              recoveryNone,
		memoryHorizon:         16,
	})
	require.NoError(t, err)

	c.count = 100
	c.observe(true)

	assert.Equal(t, 99, c.count)
	assert.Zero(t, c.fractionalCredit)
}

func TestControllerFractionalEasingDecaysCreditWhenUnhealthy(t *testing.T) {
	c, err := newController(controllerConfig{
		easingMode:            easingFractional,
		easingLogBase:         2.5,
		fractionalStrength:    0.25,
		fractionalCreditDecay: 0.5,
		recovery:              recoveryNone,
		memoryHorizon:         16,
	})
	require.NoError(t, err)

	c.count = 50
	c.fractionalCredit = 0.8
	c.observe(false)

	assert.Equal(t, 51, c.count)
	assert.InDelta(t, 0.4, c.fractionalCredit, 1e-12)
}

func TestControllerFractionalEasingDoesNotChangeColdStartGrowth(t *testing.T) {
	c, err := newController(controllerConfig{
		easingMode:            easingFractional,
		easingLogBase:         2.5,
		fractionalStrength:    0.5,
		fractionalCreditDecay: 0.9,
		recovery:              recoveryGatedReplay,
		retention:             0.5,
		memoryHorizon:         16,
	})
	require.NoError(t, err)

	transition := c.observe(false)

	assert.Equal(t, 2, c.count)
	assert.Equal(t, 1, transition.ordinaryIncrease)
	assert.Zero(t, transition.recoveryIncrease)
}

func TestControllerFractionalEasingRecordsActualExcessForGatedReplay(t *testing.T) {
	c, err := newController(controllerConfig{
		easingMode:            easingFractional,
		easingLogBase:         2,
		fractionalStrength:    0.5,
		fractionalCreditDecay: 0.5,
		recovery:              recoveryGatedReplay,
		retention:             0.5,
		memoryHorizon:         16,
	})
	require.NoError(t, err)

	c.count = 100
	c.observe(true)
	c.observe(true)

	assert.Equal(t, 97, c.count)
	assert.Equal(t, 1, c.excessEasing)
}

func TestControllerColdStartUsesNormalGrowth(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryPartialReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	transition := c.observe(false)

	assert.Equal(t, 2, c.count)
	assert.Equal(t, 1, transition.ordinaryIncrease)
	assert.Zero(t, transition.recoveryIncrease)
}

func TestControllerPartialReplayUsesEpisodeIncrease(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryPartialReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 25
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 1

	transition := c.observe(false)

	assert.Equal(t, 63, c.count)
	assert.Equal(t, 62, c.episodeStart)
	assert.Equal(t, 37, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)
}

func TestControllerPartialReplayMemoryExpires(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryPartialReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 25
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 16

	transition := c.observe(false)

	assert.Equal(t, 26, c.count)
	assert.Zero(t, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)
}

func TestControllerExcessReplayDoesNotRecoverBaselineEasing(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryExcessReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 25
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 1

	transition := c.observe(false)

	assert.Equal(t, 26, c.count)
	assert.Zero(t, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)
}

func TestControllerExcessReplayCapsRecoveryAtExcessEasing(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 2.5,
		recovery:      recoveryExcessReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 60
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 1
	c.excessEasing = 1

	transition := c.observe(false)

	assert.Equal(t, 62, c.count)
	assert.Equal(t, 1, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)
	assert.Zero(t, c.excessEasing)
}

func TestControllerExcessReplayRecordsOnlyEasingBeyondBaseThree(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 2.5,
		recovery:      recoveryExcessReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 100
	c.observe(true)
	assert.Equal(t, 98, c.count)
	assert.Equal(t, 1, c.excessEasing)

	c.count = 50
	c.observe(true)
	assert.Equal(t, 49, c.count)
	assert.Equal(t, 1, c.excessEasing)
}

func TestControllerGatedReplayDoesNotRecoverWithoutExcessEasing(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryGatedReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 25
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 1

	transition := c.observe(false)

	assert.Equal(t, 26, c.count)
	assert.Zero(t, transition.recoveryIncrease)
}

func TestControllerGatedReplayRecordsExcessEasing(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 2.5,
		recovery:      recoveryGatedReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 100
	c.observe(true)

	assert.Equal(t, 98, c.count)
	assert.Equal(t, 1, c.excessEasing)
}

func TestControllerGatedReplayUsesFullReplayAfterExcessEasing(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 2.5,
		recovery:      recoveryGatedReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 60
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 1
	c.excessEasing = 1

	transition := c.observe(false)

	assert.Equal(t, 63, c.count)
	assert.Equal(t, 2, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)
	assert.Zero(t, c.excessEasing)
}

func TestControllerPartialReplayUpdatesFromEachEpisode(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryPartialReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 25
	c.recentEpisodeStart = 25
	c.recentEpisodeIncrease = 75
	c.memoryAge = 1

	c.observe(false)
	assert.Equal(t, 62, c.episodeStart)

	c.count = 100
	c.observe(true)
	assert.Equal(t, 38, c.recentEpisodeIncrease)

	c.count = 25
	transition := c.observe(false)
	assert.Equal(t, 81, c.episodeStart)
	assert.Equal(t, 56, transition.recoveryIncrease)
}

func TestControllerReversibleDebtOnlyRestoresEasedCount(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryEasingDebt,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 85
	c.easingDebt = []int{1, 2, 4, 8}

	transition := c.observe(false)
	assert.Equal(t, 94, c.count)
	assert.Equal(t, 8, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)

	transition = c.observe(false)
	assert.Equal(t, 99, c.count)
	assert.Equal(t, 4, transition.recoveryIncrease)
	assert.Equal(t, 1, transition.ordinaryIncrease)
}

func TestControllerRecordsEpisodeIncreaseBeforeEasing(t *testing.T) {
	c, err := newController(controllerConfig{
		easingLogBase: 3,
		recovery:      recoveryPartialReplay,
		retention:     0.5,
		memoryHorizon: 16,
	})
	require.NoError(t, err)

	c.count = 60
	c.episodeActive = true
	c.episodeStart = 25

	transition := c.observe(true)

	assert.Equal(t, 35, c.recentEpisodeIncrease)
	assert.Equal(t, 25, c.recentEpisodeStart)
	assert.Equal(t, 59, c.count)
	assert.Equal(t, 1, transition.easingDecrease)
}

func TestRunSimulationIsDeterministic(t *testing.T) {
	cfg := simulationConfig{
		steps:         1_000,
		warmup:        100,
		runs:          2,
		seed:          42,
		requiredCount: 100,
		stepCount:     60,
		noise: noiseConfig{
			model:  noiseIID,
			stdDev: 8,
		},
		adaptationBand: 0.1,
		profile:        profileStationary,
		controller: controllerConfig{
			easingLogBase: 2.5,
			recovery:      recoveryPartialReplay,
			retention:     0.5,
			memoryHorizon: 16,
		},
	}

	first, err := runSimulation(cfg)
	require.NoError(t, err)
	second, err := runSimulation(cfg)
	require.NoError(t, err)

	assert.Equal(t, first, second)
	assert.Positive(t, first.meanAbsoluteError)
	assert.Positive(t, first.transitionsPerK)
}

func TestRunSimulationMeasuresDownUpAdaptation(t *testing.T) {
	result, err := runSimulation(simulationConfig{
		steps:         3_000,
		warmup:        100,
		runs:          2,
		seed:          42,
		requiredCount: 100,
		stepCount:     60,
		noise: noiseConfig{
			model: noiseIID,
		},
		adaptationBand: 0.1,
		profile:        profileDownUp,
		controller: controllerConfig{
			easingLogBase: 2.5,
			recovery:      recoveryPartialReplay,
			retention:     0.5,
			memoryHorizon: 16,
		},
	})
	require.NoError(t, err)

	assert.GreaterOrEqual(t, result.releaseSteps, float64(0))
	assert.GreaterOrEqual(t, result.recoverySteps, float64(0))
}

func TestNoiseProcessIID(t *testing.T) {
	actualRNG := rand.New(rand.NewSource(42))
	expectedRNG := rand.New(rand.NewSource(42))
	noise, err := newNoiseProcess(noiseConfig{
		model:  noiseIID,
		stdDev: 8,
	})
	require.NoError(t, err)

	assert.Equal(t, expectedRNG.NormFloat64()*8, noise.next(actualRNG))
	assert.Equal(t, expectedRNG.NormFloat64()*8, noise.next(actualRNG))
}

func TestNoiseProcessAR1(t *testing.T) {
	actualRNG := rand.New(rand.NewSource(42))
	expectedRNG := rand.New(rand.NewSource(42))
	noise, err := newNoiseProcess(noiseConfig{
		model:       noiseAR1,
		stdDev:      8,
		correlation: 0.9,
	})
	require.NoError(t, err)

	innovationScale := 8 * math.Sqrt(1-0.9*0.9)
	first := expectedRNG.NormFloat64() * innovationScale
	second := 0.9*first + expectedRNG.NormFloat64()*innovationScale

	assert.InDelta(t, first, noise.next(actualRNG), 1e-12)
	assert.InDelta(t, second, noise.next(actualRNG), 1e-12)
}

func TestNoiseProcessRejectsInvalidCorrelation(t *testing.T) {
	_, err := newNoiseProcess(noiseConfig{
		model:       noiseAR1,
		stdDev:      8,
		correlation: 1,
	})

	assert.ErrorContains(t, err, "correlation")
}

func TestBuildSimulationConfigsSweepsCountsAndNoise(t *testing.T) {
	configs, err := buildSimulationConfigs(commandConfig{
		steps:             1_000,
		warmup:            100,
		runs:              2,
		seed:              42,
		requiredCount:     100,
		requiredCounts:    "25,100",
		stepCount:         60,
		noiseFraction:     0.08,
		noiseModels:       "iid,ar1",
		noiseCorrelations: "0.5,0.9",
		adaptationBand:    0.1,
		profile:           string(profileStationary),
		easingBases:       "3,2.5",
		recoveries:        "none,replay",
		retentions:        "0.5",
		memoryHorizon:     16,
	})
	require.NoError(t, err)

	assert.Len(t, configs, 24)
	assert.Equal(t, float64(25), configs[0].requiredCount)
	assert.Equal(t, noiseIID, configs[0].noise.model)
	assert.Equal(t, float64(2), configs[0].noise.stdDev)
	assert.Equal(t, float64(100), configs[len(configs)-1].requiredCount)
	assert.Equal(t, noiseAR1, configs[len(configs)-1].noise.model)
	assert.Equal(t, float64(8), configs[len(configs)-1].noise.stdDev)
	assert.Equal(t, 0.9, configs[len(configs)-1].noise.correlation)
}

func TestBuildSimulationConfigsIncludesExcessReplayRetention(t *testing.T) {
	configs, err := buildSimulationConfigs(commandConfig{
		steps:             1_000,
		warmup:            100,
		runs:              2,
		seed:              42,
		requiredCount:     100,
		stepCount:         60,
		noiseStdDev:       8,
		noiseModels:       "iid",
		noiseCorrelations: "0.5,0.9",
		adaptationBand:    0.1,
		profile:           string(profileStationary),
		easingBases:       "2.5",
		recoveries:        "excess-replay",
		retentions:        "0.25,0.5",
		memoryHorizon:     16,
	})
	require.NoError(t, err)

	require.Len(t, configs, 2)
	assert.Equal(t, recoveryExcessReplay, configs[0].controller.recovery)
	assert.Equal(t, 0.25, configs[0].controller.retention)
	assert.Equal(t, 0.5, configs[1].controller.retention)
}

func TestBuildSimulationConfigsIncludesGatedReplayRetention(t *testing.T) {
	configs, err := buildSimulationConfigs(commandConfig{
		steps:             1_000,
		warmup:            100,
		runs:              2,
		seed:              42,
		requiredCount:     100,
		stepCount:         60,
		noiseStdDev:       8,
		noiseModels:       "iid",
		noiseCorrelations: "0.5,0.9",
		adaptationBand:    0.1,
		profile:           string(profileStationary),
		easingBases:       "2.5",
		recoveries:        "gated-replay",
		retentions:        "0.25,0.5",
		memoryHorizon:     16,
	})
	require.NoError(t, err)

	require.Len(t, configs, 2)
	assert.Equal(t, recoveryGatedReplay, configs[0].controller.recovery)
	assert.Equal(t, 0.25, configs[0].controller.retention)
	assert.Equal(t, 0.5, configs[1].controller.retention)
}

func TestBuildSimulationConfigsSweepsFractionalEasing(t *testing.T) {
	configs, err := buildSimulationConfigs(commandConfig{
		steps:                  1_000,
		warmup:                 100,
		runs:                   2,
		seed:                   42,
		requiredCount:          100,
		stepCount:              60,
		noiseStdDev:            8,
		noiseModels:            "iid",
		noiseCorrelations:      "0.5,0.9",
		adaptationBand:         0.1,
		profile:                string(profileStationary),
		easingModes:            "integer,fractional",
		easingBases:            "2.5",
		fractionalStrengths:    "0.1,0.25",
		fractionalCreditDecays: "0.5,0.9",
		recoveries:             "none",
		retentions:             "0.5",
		memoryHorizon:          16,
	})
	require.NoError(t, err)

	require.Len(t, configs, 5)
	assert.Equal(t, easingInteger, configs[0].controller.easingMode)
	assert.Equal(t, easingFractional, configs[1].controller.easingMode)
	assert.Equal(t, 0.1, configs[1].controller.fractionalStrength)
	assert.Equal(t, 0.5, configs[1].controller.fractionalCreditDecay)
	assert.Equal(t, 0.25, configs[4].controller.fractionalStrength)
	assert.Equal(t, 0.9, configs[4].controller.fractionalCreditDecay)
}
