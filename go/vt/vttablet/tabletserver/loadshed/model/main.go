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
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	recoveryStrategy string
	profileName      string
	noiseModel       string
	easingMode       string

	noiseConfig struct {
		model       noiseModel
		stdDev      float64
		correlation float64
	}

	noiseProcess struct {
		cfg   noiseConfig
		value float64
	}

	controllerConfig struct {
		easingMode            easingMode
		easingLogBase         float64
		fractionalStrength    float64
		fractionalCreditDecay float64
		recovery              recoveryStrategy
		retention             float64
		memoryHorizon         int
	}

	controller struct {
		count int
		cfg   controllerConfig

		episodeActive bool
		episodeStart  int

		recentEpisodeStart    int
		recentEpisodeIncrease int
		memoryAge             int

		excessEasing     int
		fractionalCredit float64

		easingDebt    []int
		easingDebtAge int
	}

	transition struct {
		ordinaryIncrease int
		recoveryIncrease int
		easingDecrease   int
	}

	simulationConfig struct {
		steps          int
		warmup         int
		runs           int
		seed           int64
		requiredCount  float64
		stepCount      float64
		noise          noiseConfig
		adaptationBand float64
		profile        profileName
		controller     controllerConfig
	}

	simulationResult struct {
		requiredCount         float64
		noiseModel            noiseModel
		noiseCorrelation      float64
		recovery              recoveryStrategy
		easingMode            easingMode
		base                  float64
		fractionalStrength    float64
		fractionalCreditDecay float64
		retention             float64

		meanCount            float64
		meanAbsoluteError    float64
		p95AbsoluteError     float64
		meanUnder            float64
		meanOver             float64
		healthyFraction      float64
		transitionsPerK      float64
		ordinaryIncreasePerK float64
		recoveryIncreasePerK float64
		easingDecreasePerK   float64
		releaseSteps         float64
		recoverySteps        float64
	}

	simulationAccumulator struct {
		samples int

		countSum             float64
		absoluteErrorSum     float64
		underSum             float64
		overSum              float64
		healthy              int
		transitions          int
		ordinaryIncrease     int
		recoveryIncrease     int
		easingDecrease       int
		absoluteErrors       []float64
		releaseStepsSum      int
		releaseStepsSamples  int
		recoveryStepsSum     int
		recoveryStepsSamples int
	}

	commandConfig struct {
		steps                  int
		warmup                 int
		runs                   int
		seed                   int64
		requiredCount          float64
		requiredCounts         string
		stepCount              float64
		noiseStdDev            float64
		noiseFraction          float64
		noiseModels            string
		noiseCorrelations      string
		adaptationBand         float64
		profile                string
		easingModes            string
		easingBases            string
		fractionalStrengths    string
		fractionalCreditDecays string
		recoveries             string
		retentions             string
		memoryHorizon          int
	}
)

const (
	recoveryNone          recoveryStrategy = "none"
	recoveryEasingDebt    recoveryStrategy = "debt"
	recoveryPartialReplay recoveryStrategy = "replay"
	recoveryExcessReplay  recoveryStrategy = "excess-replay"
	recoveryGatedReplay   recoveryStrategy = "gated-replay"

	profileStationary profileName = "stationary"
	profileDownUp     profileName = "down-up"

	noiseIID noiseModel = "iid"
	noiseAR1 noiseModel = "ar1"

	easingInteger    easingMode = "integer"
	easingFractional easingMode = "fractional"
)

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags() commandConfig {
	cfg := commandConfig{}
	flag.IntVar(&cfg.steps, "steps", 30_000, "number of controller intervals per run")
	flag.IntVar(&cfg.warmup, "warmup", 3_000, "number of initial intervals excluded from stationary metrics")
	flag.IntVar(&cfg.runs, "runs", 20, "number of independent random seeds")
	flag.Int64Var(&cfg.seed, "seed", 1, "first random seed")
	flag.Float64Var(&cfg.requiredCount, "required-count", 100, "required count in the high-load phase")
	flag.StringVar(&cfg.requiredCounts, "required-counts", "", "optional comma-separated required counts to sweep")
	flag.Float64Var(&cfg.stepCount, "step-count", 60, "required count in the low-load phase")
	flag.Float64Var(&cfg.noiseStdDev, "noise-stddev", 8, "standard deviation of observed required-count noise")
	flag.Float64Var(&cfg.noiseFraction, "noise-fraction", 0, "noise standard deviation as a fraction of required count; overrides noise-stddev when positive")
	flag.StringVar(&cfg.noiseModels, "noise-models", string(noiseIID), "comma-separated noise models: iid or ar1")
	flag.StringVar(&cfg.noiseCorrelations, "noise-correlations", "0.5,0.9", "comma-separated AR(1) correlations")
	flag.Float64Var(&cfg.adaptationBand, "adaptation-band", 0.1, "fractional band used for step adaptation time")
	flag.StringVar(&cfg.profile, "profile", string(profileStationary), "workload profile: stationary or down-up")
	flag.StringVar(&cfg.easingModes, "easing-modes", string(easingInteger), "comma-separated easing modes: integer or fractional")
	flag.StringVar(&cfg.easingBases, "easing-bases", "3,2.6,2.55,2.5,2.4,2.25", "comma-separated easing log bases")
	flag.StringVar(&cfg.fractionalStrengths, "fractional-strengths", "0.1,0.25,0.5", "comma-separated fractional excess-easing strengths")
	flag.StringVar(&cfg.fractionalCreditDecays, "fractional-credit-decays", "0,0.5,0.9", "comma-separated fractional-credit decay factors applied on unhealthy observations")
	flag.StringVar(&cfg.recoveries, "recoveries", "none,debt,replay,excess-replay,gated-replay", "comma-separated recovery strategies")
	flag.StringVar(&cfg.retentions, "retentions", "0.25,0.5,0.75", "comma-separated partial-replay retention values")
	flag.IntVar(&cfg.memoryHorizon, "memory-horizon", 16, "recovery-memory lifetime in controller intervals")
	flag.Parse()
	return cfg
}

func run(cfg commandConfig) error {
	configs, err := buildSimulationConfigs(cfg)
	if err != nil {
		return err
	}

	fmt.Println("profile\trequired_count\tnoise_model\tcorrelation\teasing_mode\tbase\tfractional_strength\tfractional_credit_decay\trecovery\tretention\tmean_count\tmean_abs_error\tp95_abs_error\tmean_under\tmean_over\thealthy_fraction\ttransitions_per_1k\tordinary_increase_per_1k\trecovery_increase_per_1k\teasing_decrease_per_1k\trelease_steps\trecovery_steps")
	for _, simulationCfg := range configs {
		result, err := runSimulation(simulationCfg)
		if err != nil {
			return err
		}
		fmt.Printf(
			"%s\t%.3f\t%s\t%.2f\t%s\t%.3f\t%.3f\t%.3f\t%s\t%.2f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.4f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n",
			simulationCfg.profile,
			result.requiredCount,
			result.noiseModel,
			result.noiseCorrelation,
			result.easingMode,
			result.base,
			result.fractionalStrength,
			result.fractionalCreditDecay,
			result.recovery,
			result.retention,
			result.meanCount,
			result.meanAbsoluteError,
			result.p95AbsoluteError,
			result.meanUnder,
			result.meanOver,
			result.healthyFraction,
			result.transitionsPerK,
			result.ordinaryIncreasePerK,
			result.recoveryIncreasePerK,
			result.easingDecreasePerK,
			result.releaseSteps,
			result.recoverySteps,
		)
	}
	return nil
}

func buildSimulationConfigs(cfg commandConfig) ([]simulationConfig, error) {
	if cfg.easingModes == "" {
		cfg.easingModes = string(easingInteger)
	}
	easingModes, err := parseEasingModeList(cfg.easingModes)
	if err != nil {
		return nil, err
	}
	bases, err := parseFloatList(cfg.easingBases)
	if err != nil {
		return nil, err
	}
	fractionalStrengths := []float64{0}
	fractionalCreditDecays := []float64{0}
	if slices.Contains(easingModes, easingFractional) {
		if cfg.fractionalStrengths == "" {
			cfg.fractionalStrengths = "0.1"
		}
		fractionalStrengths, err = parseFloatList(cfg.fractionalStrengths)
		if err != nil {
			return nil, err
		}
		if cfg.fractionalCreditDecays == "" {
			cfg.fractionalCreditDecays = "0.5"
		}
		fractionalCreditDecays, err = parseFloatList(cfg.fractionalCreditDecays)
		if err != nil {
			return nil, err
		}
	}
	recoveries, err := parseRecoveryList(cfg.recoveries)
	if err != nil {
		return nil, err
	}
	retentions, err := parseFloatList(cfg.retentions)
	if err != nil {
		return nil, err
	}
	profile, err := parseProfile(cfg.profile)
	if err != nil {
		return nil, err
	}
	requiredCounts := []float64{cfg.requiredCount}
	if cfg.requiredCounts != "" {
		requiredCounts, err = parseFloatList(cfg.requiredCounts)
		if err != nil {
			return nil, err
		}
	}
	noiseModels, err := parseNoiseModelList(cfg.noiseModels)
	if err != nil {
		return nil, err
	}
	noiseCorrelations, err := parseFloatList(cfg.noiseCorrelations)
	if err != nil {
		return nil, err
	}
	if cfg.steps <= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "steps must be positive: %d", cfg.steps)
	}
	if cfg.warmup < 0 || cfg.warmup >= cfg.steps {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "warmup must be in [0, steps): %d", cfg.warmup)
	}
	if cfg.runs <= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "runs must be positive: %d", cfg.runs)
	}
	for _, requiredCount := range requiredCounts {
		if requiredCount < 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "required counts must be at least 1: %f", requiredCount)
		}
	}
	if cfg.stepCount < 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "step count must be at least 1: %f", cfg.stepCount)
	}
	if cfg.noiseStdDev < 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "noise standard deviation cannot be negative: %f", cfg.noiseStdDev)
	}
	if cfg.noiseFraction < 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "noise fraction cannot be negative: %f", cfg.noiseFraction)
	}
	if cfg.adaptationBand < 0 || cfg.adaptationBand >= 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "adaptation band must be in [0, 1): %f", cfg.adaptationBand)
	}

	result := make([]simulationConfig, 0, len(requiredCounts)*len(noiseModels)*len(bases)*len(recoveries))
	for _, requiredCount := range requiredCounts {
		for _, model := range noiseModels {
			correlations := []float64{0}
			if model == noiseAR1 {
				correlations = noiseCorrelations
			}
			for _, correlation := range correlations {
				noiseStdDev := cfg.noiseStdDev
				if cfg.noiseFraction > 0 {
					noiseStdDev = requiredCount * cfg.noiseFraction
				}
				noise := noiseConfig{
					model:       model,
					stdDev:      noiseStdDev,
					correlation: correlation,
				}
				if _, err := newNoiseProcess(noise); err != nil {
					return nil, err
				}
				for _, base := range bases {
					for _, mode := range easingModes {
						modeStrengths := []float64{0}
						modeDecays := []float64{0}
						if mode == easingFractional {
							modeStrengths = fractionalStrengths
							modeDecays = fractionalCreditDecays
						}
						for _, strength := range modeStrengths {
							for _, decay := range modeDecays {
								for _, recovery := range recoveries {
									recoveryRetentions := []float64{0}
									if recovery == recoveryPartialReplay || recovery == recoveryExcessReplay || recovery == recoveryGatedReplay {
										recoveryRetentions = retentions
									}
									for _, retention := range recoveryRetentions {
										controllerCfg := controllerConfig{
											easingMode:            mode,
											easingLogBase:         base,
											fractionalStrength:    strength,
											fractionalCreditDecay: decay,
											recovery:              recovery,
											retention:             retention,
											memoryHorizon:         cfg.memoryHorizon,
										}
										if _, err := newController(controllerCfg); err != nil {
											return nil, err
										}
										result = append(result, simulationConfig{
											steps:          cfg.steps,
											warmup:         cfg.warmup,
											runs:           cfg.runs,
											seed:           cfg.seed,
											requiredCount:  requiredCount,
											stepCount:      cfg.stepCount,
											noise:          noise,
											adaptationBand: cfg.adaptationBand,
											profile:        profile,
											controller:     controllerCfg,
										})
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return result, nil
}

func newController(cfg controllerConfig) (*controller, error) {
	if cfg.easingMode == "" {
		cfg.easingMode = easingInteger
	}
	if cfg.easingLogBase <= 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "easing log base must be greater than 1: %f", cfg.easingLogBase)
	}
	switch cfg.easingMode {
	case easingInteger:
	case easingFractional:
		if cfg.fractionalStrength < 0 || cfg.fractionalStrength > 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "fractional strength must be in [0, 1]: %f", cfg.fractionalStrength)
		}
		if cfg.fractionalCreditDecay < 0 || cfg.fractionalCreditDecay > 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "fractional credit decay must be in [0, 1]: %f", cfg.fractionalCreditDecay)
		}
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown easing mode: %s", cfg.easingMode)
	}
	switch cfg.recovery {
	case recoveryNone, recoveryEasingDebt:
	case recoveryPartialReplay, recoveryExcessReplay, recoveryGatedReplay:
		if cfg.retention < 0 || cfg.retention > 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "retention must be in [0, 1]: %f", cfg.retention)
		}
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown recovery strategy: %s", cfg.recovery)
	}
	if cfg.memoryHorizon <= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "memory horizon must be positive: %d", cfg.memoryHorizon)
	}
	return &controller{
		count: 1,
		cfg:   cfg,
	}, nil
}

func newNoiseProcess(cfg noiseConfig) (*noiseProcess, error) {
	if cfg.stdDev < 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "noise standard deviation cannot be negative: %f", cfg.stdDev)
	}
	switch cfg.model {
	case noiseIID:
		if cfg.correlation != 0 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "IID noise correlation must be zero: %f", cfg.correlation)
		}
	case noiseAR1:
		if cfg.correlation < 0 || cfg.correlation >= 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "AR(1) correlation must be in [0, 1): %f", cfg.correlation)
		}
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown noise model: %s", cfg.model)
	}
	return &noiseProcess{cfg: cfg}, nil
}

func (n *noiseProcess) next(rng *rand.Rand) float64 {
	if n.cfg.model == noiseIID {
		return rng.NormFloat64() * n.cfg.stdDev
	}
	innovationScale := n.cfg.stdDev * math.Sqrt(1-n.cfg.correlation*n.cfg.correlation)
	n.value = n.cfg.correlation*n.value + rng.NormFloat64()*innovationScale
	return n.value
}

func (c *controller) observe(healthy bool) transition {
	if healthy {
		return c.observeHealthy()
	}
	return c.observeUnhealthy()
}

func (c *controller) observeHealthy() transition {
	result := transition{}
	if c.episodeActive {
		c.recentEpisodeStart = c.episodeStart
		c.recentEpisodeIncrease = max(c.count-c.episodeStart, 0)
		c.memoryAge = 0
		c.episodeActive = false
	} else if c.recentEpisodeIncrease > 0 {
		c.memoryAge++
	}

	eased := easeCount(c.count, c.cfg.easingLogBase)
	excessEasing := 0
	if c.cfg.easingMode == easingFractional {
		baselineEased := easeCount(c.count, 3)
		c.fractionalCredit += c.cfg.fractionalStrength * max(rawEaseStep(c.count, c.cfg.easingLogBase)-rawEaseStep(c.count, 3), 0)
		requestedExtra := int(math.Floor(c.fractionalCredit))
		excessEasing = min(requestedExtra, max(baselineEased-1, 0))
		c.fractionalCredit -= float64(excessEasing)
		eased = baselineEased - excessEasing
	}
	result.easingDecrease = c.count - eased
	if c.cfg.recovery == recoveryExcessReplay || c.cfg.recovery == recoveryGatedReplay {
		if c.cfg.easingMode == easingFractional {
			c.excessEasing += excessEasing
		} else {
			baselineEased := easeCount(c.count, 3)
			c.excessEasing += max(result.easingDecrease-(c.count-baselineEased), 0)
		}
	}
	c.count = eased

	if c.cfg.recovery == recoveryEasingDebt && result.easingDecrease > 0 {
		c.easingDebt = append(c.easingDebt, result.easingDecrease)
		c.easingDebtAge = 0
	} else if len(c.easingDebt) > 0 {
		c.easingDebtAge++
		if c.easingDebtAge >= c.cfg.memoryHorizon {
			c.easingDebt = c.easingDebt[:0]
		}
	}
	return result
}

func (c *controller) observeUnhealthy() transition {
	result := transition{}
	if c.cfg.easingMode == easingFractional {
		c.fractionalCredit *= c.cfg.fractionalCreditDecay
	}
	if !c.episodeActive {
		replayAllowed := c.cfg.recovery == recoveryPartialReplay || c.cfg.recovery == recoveryExcessReplay
		if c.cfg.recovery == recoveryGatedReplay {
			replayAllowed = c.excessEasing > 0
		}
		if replayAllowed && c.recentMemoryValid() {
			restore := c.recentEpisodeStart + int(c.cfg.retention*float64(c.recentEpisodeIncrease))
			if restore > c.count {
				result.recoveryIncrease = restore - c.count
				if c.cfg.recovery == recoveryExcessReplay {
					result.recoveryIncrease = min(result.recoveryIncrease, c.excessEasing)
					c.count += result.recoveryIncrease
				} else {
					c.count = restore
				}
			}
		}
		if c.cfg.recovery == recoveryExcessReplay || c.cfg.recovery == recoveryGatedReplay {
			c.excessEasing = 0
		}
		c.episodeStart = c.count
		c.episodeActive = true
	}

	if c.cfg.recovery == recoveryEasingDebt && len(c.easingDebt) > 0 {
		last := len(c.easingDebt) - 1
		result.recoveryIncrease = c.easingDebt[last]
		c.easingDebt = c.easingDebt[:last]
		c.count += result.recoveryIncrease
		c.easingDebtAge = 0
	}

	c.count++
	result.ordinaryIncrease = 1
	return result
}

func (c *controller) recentMemoryValid() bool {
	return c.recentEpisodeIncrease > 1 && c.memoryAge < c.cfg.memoryHorizon
}

func easeCount(count int, base float64) int {
	if count <= 1 {
		return 1
	}
	step := int(math.Log(float64(count)) / math.Log(base) / base)
	return max(count-max(step, 1), 1)
}

func rawEaseStep(count int, base float64) float64 {
	if count <= 1 {
		return 0
	}
	return math.Log(float64(count)) / math.Log(base) / base
}

func runSimulation(cfg simulationConfig) (simulationResult, error) {
	if cfg.steps <= 0 || cfg.runs <= 0 {
		return simulationResult{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "steps and runs must be positive")
	}
	accumulator := simulationAccumulator{}
	for run := 0; run < cfg.runs; run++ {
		c, err := newController(cfg.controller)
		if err != nil {
			return simulationResult{}, err
		}
		noise, err := newNoiseProcess(cfg.noise)
		if err != nil {
			return simulationResult{}, err
		}
		rng := rand.New(rand.NewSource(cfg.seed + int64(run)))
		var (
			havePreviousHealth bool
			previousHealthy    bool
			releaseRecorded    bool
			recoveryRecorded   bool
		)
		downAt := cfg.steps / 3
		upAt := 2 * cfg.steps / 3

		for step := 0; step < cfg.steps; step++ {
			required := requiredAt(cfg, step)
			observedRequired := max(required+noise.next(rng), 1)
			countBefore := c.count
			healthy := float64(countBefore) >= observedRequired
			transition := c.observe(healthy)

			if step >= cfg.warmup {
				accumulator.addSample(float64(countBefore), required, healthy, transition)
				if havePreviousHealth && healthy != previousHealthy {
					accumulator.transitions++
				}
			}
			havePreviousHealth = true
			previousHealthy = healthy

			if cfg.profile == profileDownUp {
				bandDown := cfg.stepCount * cfg.adaptationBand
				if !releaseRecorded && step >= downAt && float64(countBefore) <= cfg.stepCount+bandDown {
					accumulator.releaseStepsSum += step - downAt
					accumulator.releaseStepsSamples++
					releaseRecorded = true
				}
				bandUp := cfg.requiredCount * cfg.adaptationBand
				if !recoveryRecorded && step >= upAt && float64(countBefore) >= cfg.requiredCount-bandUp {
					accumulator.recoveryStepsSum += step - upAt
					accumulator.recoveryStepsSamples++
					recoveryRecorded = true
				}
			}
		}
	}
	return accumulator.result(cfg), nil
}

func (a *simulationAccumulator) addSample(count, required float64, healthy bool, transition transition) {
	errorValue := count - required
	absoluteError := math.Abs(errorValue)
	a.samples++
	a.countSum += count
	a.absoluteErrorSum += absoluteError
	a.absoluteErrors = append(a.absoluteErrors, absoluteError)
	if errorValue < 0 {
		a.underSum -= errorValue
	} else {
		a.overSum += errorValue
	}
	if healthy {
		a.healthy++
	}
	a.ordinaryIncrease += transition.ordinaryIncrease
	a.recoveryIncrease += transition.recoveryIncrease
	a.easingDecrease += transition.easingDecrease
}

func (a *simulationAccumulator) result(cfg simulationConfig) simulationResult {
	sort.Float64s(a.absoluteErrors)
	scale := 1000 / float64(a.samples)
	result := simulationResult{
		requiredCount:         cfg.requiredCount,
		noiseModel:            cfg.noise.model,
		noiseCorrelation:      cfg.noise.correlation,
		recovery:              cfg.controller.recovery,
		easingMode:            cfg.controller.easingMode,
		base:                  cfg.controller.easingLogBase,
		fractionalStrength:    cfg.controller.fractionalStrength,
		fractionalCreditDecay: cfg.controller.fractionalCreditDecay,
		retention:             cfg.controller.retention,
		meanCount:             a.countSum / float64(a.samples),
		meanAbsoluteError:     a.absoluteErrorSum / float64(a.samples),
		p95AbsoluteError:      percentile(a.absoluteErrors, 0.95),
		meanUnder:             a.underSum / float64(a.samples),
		meanOver:              a.overSum / float64(a.samples),
		healthyFraction:       float64(a.healthy) / float64(a.samples),
		transitionsPerK:       float64(a.transitions) * scale,
		ordinaryIncreasePerK:  float64(a.ordinaryIncrease) * scale,
		recoveryIncreasePerK:  float64(a.recoveryIncrease) * scale,
		easingDecreasePerK:    float64(a.easingDecrease) * scale,
		releaseSteps:          -1,
		recoverySteps:         -1,
	}
	if a.releaseStepsSamples > 0 {
		result.releaseSteps = float64(a.releaseStepsSum) / float64(a.releaseStepsSamples)
	}
	if a.recoveryStepsSamples > 0 {
		result.recoverySteps = float64(a.recoveryStepsSum) / float64(a.recoveryStepsSamples)
	}
	return result
}

func requiredAt(cfg simulationConfig, step int) float64 {
	if cfg.profile != profileDownUp {
		return cfg.requiredCount
	}
	if step >= cfg.steps/3 && step < 2*cfg.steps/3 {
		return cfg.stepCount
	}
	return cfg.requiredCount
}

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(values)))) - 1
	return values[max(index, 0)]
}

func parseFloatList(value string) ([]float64, error) {
	parts := strings.Split(value, ",")
	result := make([]float64, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			return nil, vterrors.Wrapf(err, "invalid floating-point list value %s", part)
		}
		result = append(result, parsed)
	}
	if len(result) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "floating-point list cannot be empty")
	}
	return result, nil
}

func parseRecoveryList(value string) ([]recoveryStrategy, error) {
	parts := strings.Split(value, ",")
	result := make([]recoveryStrategy, 0, len(parts))
	for _, part := range parts {
		recovery := recoveryStrategy(strings.TrimSpace(part))
		switch recovery {
		case recoveryNone, recoveryEasingDebt, recoveryPartialReplay, recoveryExcessReplay, recoveryGatedReplay:
			result = append(result, recovery)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown recovery strategy: %s", recovery)
		}
	}
	if len(result) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "recovery list cannot be empty")
	}
	return result, nil
}

func parseEasingModeList(value string) ([]easingMode, error) {
	parts := strings.Split(value, ",")
	result := make([]easingMode, 0, len(parts))
	for _, part := range parts {
		mode := easingMode(strings.TrimSpace(part))
		switch mode {
		case easingInteger, easingFractional:
			result = append(result, mode)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown easing mode: %s", mode)
		}
	}
	if len(result) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "easing mode list cannot be empty")
	}
	return result, nil
}

func parseNoiseModelList(value string) ([]noiseModel, error) {
	parts := strings.Split(value, ",")
	result := make([]noiseModel, 0, len(parts))
	for _, part := range parts {
		model := noiseModel(strings.TrimSpace(part))
		switch model {
		case noiseIID, noiseAR1:
			result = append(result, model)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown noise model: %s", model)
		}
	}
	if len(result) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "noise model list cannot be empty")
	}
	return result, nil
}

func parseProfile(value string) (profileName, error) {
	profile := profileName(value)
	switch profile {
	case profileStationary, profileDownUp:
		return profile, nil
	default:
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown profile: %s", profile)
	}
}
