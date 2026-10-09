# CoDel Count Recovery Modeling

## Problem

For a given workload, CoDel has a range of drop counts that can keep queue
latency near the target without introducing unnecessary errors. The input load
is noisy, so the queue naturally alternates between healthy and unhealthy even
when the workload is stationary.

The current implementation eases the count slowly during healthy intervals.
This keeps the count near its recent working value and prevents ordinary load
variance from causing large changes. It also makes recovery slow after the
count has eased too far, particularly at low counts where ordinary CoDel growth
can take many drops.

More aggressive easing could adapt faster when load falls, but it must not make
a cold start more aggressive. Cold-start escalation is already controlled by
the CoDel exponent. Any additional fast recovery should therefore be limited to
aggressiveness that the controller recently removed itself.

## Goals

- Keep count near the value needed by a stationary but noisy workload.
- Adapt quickly when offered load changes materially.
- Recover quickly when recent easing reduced count too far.
- Preserve normal CoDel growth when there is no recent easing or dropping
  history.
- Avoid snapping unconditionally to a previous high count.
- Allow the controller to settle at an intermediate count after workload
  changes.

## Approach 1: Reversible Easing Debt

Treat aggressive easing as a tentative reduction that can be undone if the
queue becomes unhealthy again.

Each healthy interval reduces `count` and records the reduction as easing debt:

```text
100 -(1)→ 99 -(2)→ 97 -(4)→ 93 -(8)→ 85
```

If unhealthy conditions return, the controller restores only count removed by
recent easing:

```text
85 +(8)→ 93 +(4)→ 97
```

Once all easing debt is recovered, count resumes normal CoDel growth: one
increment per drop, paced according to the configured exponent.

### Properties

- Cold starts remain unchanged because count 1 has no easing debt.
- Upward acceleration occurs only after the controller previously reduced
  count.
- Small healthy/unhealthy fluctuations produce small reversible adjustments.
- Sustained health permits increasingly aggressive easing.
- Recovery is bounded by the previous count.

### Concern

The previous high remains the destination of recovery. Reversing easing steps
does not provide a natural way to settle between the eased count and the
previous high. Without additional damping, the controller may repeatedly move
between adjacent easing levels.

## Approach 2: Partial Replay of Episode Increase

Track how much count increased during each dropping episode:

```text
increase = episodeEnd - episodeStart
```

After count eases and the queue becomes unhealthy again, replay only a fraction
of that increase:

```text
restore = episodeStart + retention*increase
count   = max(easedCount, restore)
```

With `retention = 0.5`:

```text
episode: 25 → 100, increase=75
ease:    100 → 25
restore: 62

episode: 62 → 100, increase=38
restore: 81

episode: 81 → 100, increase=19
restore: 90
```

If the required count has fallen:

```text
episode: 62 → 70, increase=8
restore: 66
```

The episode start must be the count selected when entering the dropping
episode, before that episode increments count. The episode end must be captured
when the queue first becomes healthy, before easing mutates count.

### Properties

- Cold-start behavior remains unchanged when no recent episode exists.
- Recovery strength reflects how much correction the preceding episode
  required.
- Repeated episodes approach the working count instead of always snapping to
  the previous high.
- Smaller workload changes naturally produce smaller restorations.
- `retention` controls convergence speed and overshoot risk.
- The current eased count remains a floor, so becoming unhealthy never reduces
  count.
- Episode memory can expire using CoDel's existing 16-interval recency window.

### Concern

Noisy episode boundaries may produce noisy increase measurements. Retention,
memory expiration, and potentially smoothing the observed increase must be
evaluated together rather than selected independently.

## Modeling Plan

### 1. Establish the Baselines

Model these controllers:

1. Current log-base easing.
2. Normal CoDel count growth without easing.
3. Reversible easing debt.
4. Partial replay of episode increase.

All models must share the same queue, health classification, control law,
target, interval, exponent, and timer behavior. Only count recovery and easing
may differ.

### 2. Build a Deterministic Controller Model

Start with a discrete interval model in which each interval reports healthy or
unhealthy. This isolates the count update rules from queueing effects.

Exercise:

- Alternating healthy and unhealthy observations.
- Runs of healthy observations of different lengths.
- Runs of unhealthy observations of different lengths.
- A stable required count.
- Step changes in the required count.
- Health observations with controlled random classification errors.

Record the complete sequence of:

- Count.
- Episode start and end.
- Episode increase.
- Easing debt.
- Chosen restoration count.
- Healthy and unhealthy transitions.

Use this model to reject formulas that have obvious fixed-point oscillations,
fail to converge, or increase aggressively from a cold start.

### 3. Define the Workload Oracle

For each stationary workload, find the minimum fixed shedding rate that keeps
the selected latency metric within its target. This is the workload's oracle
shedding rate.

Because count is transformed by the exponent, compare controllers primarily in
terms of effective scheduled drop rate:

```text
effectiveDropRate = count^exponent / interval
```

Count error can still be reported, but effective drop-rate error is comparable
across exponent values.

The oracle should be generated by sweeping fixed shedding rates after a warm-up
period and selecting the lowest rate that satisfies the latency constraint.
This gives the model a measurable definition of "correct" shedding rather than
relying on visual inspection.

### 4. Build a Queueing Simulation

Use a discrete-event queue with configurable:

- Worker capacity.
- Arrival process.
- Service-time distribution.
- CoDel target and interval.
- CoDel exponent.
- Timer delay and scheduling jitter.

The simulation should execute the same health and count transitions as the
production implementation. It should model request arrivals, grants,
completions, queue sojourn, and drops rather than converting offered load
directly into synthetic health observations.

### 5. Workload Profiles

Evaluate each controller against:

#### Stationary load

- Below capacity.
- Near capacity.
- Mild overload.
- Moderate overload.
- Heavy overload.

#### Load transitions

- Step from underload to overload.
- Step from overload to underload.
- Small changes around the current equilibrium.
- Large changes that make previous count memory stale.

#### Recurring load

- Periodic bursts with gaps shorter than one interval.
- Gaps of 1, 2, 4, 8, and 16 intervals.
- Square waves around the capacity boundary.

#### Noisy load

- Poisson arrivals with fixed service time.
- Poisson arrivals with variable service time.
- Overdispersed arrivals.
- Correlated random-walk load.
- Rare large service-time outliers.

The noise parameters should cover low, moderate, and high coefficients of
variation. Runs must use fixed seeds and enough independent seeds to distinguish
controller behavior from a favorable random sequence.

### 6. Parameter Sweeps

Sweep:

- CoDel exponent.
- Current easing log base.
- Aggressiveness of reversible easing.
- Partial-replay retention.
- Memory expiration in intervals.
- Arrival and service-time variance.
- Offered load relative to capacity.

Begin partial replay with retention values:

```text
0.25, 0.5, 0.75
```

Use the current 16-interval expiration as the initial memory horizon, then
compare shorter horizons if stale recovery causes excess shedding.

### 7. Metrics

#### Latency

- Queue-sojourn p50, p95, and p99.
- Fraction of requests exceeding target.
- Time-integrated latency above target.
- Peak latency after a load increase.

#### Errors and throughput

- Shed requests.
- Shed fraction.
- Completed throughput.
- Excess shedding relative to the workload oracle.
- Drops after a load decrease.

#### Adaptation

- Time to enter the oracle shedding-rate band after a load increase.
- Time to enter the oracle band after a load decrease.
- Maximum overshoot and undershoot.
- Recovery time after count was eased below the oracle.

#### Stability

- Median absolute distance from the oracle rate.
- Effective drop-rate variance under stationary load.
- Peak-to-peak count movement.
- Total count variation.
- Healthy/unhealthy transition frequency.
- Frequency and duration of limit cycles.

### 8. Required Scenarios

The following scenarios should be treated as acceptance cases:

1. A stationary noisy workload whose oracle count is high. The controller
   should remain near that value without repeatedly easing far below it.
2. Count has recently eased from 100 to 25 and the same overload returns.
   Recovery should be materially faster than 75 ordinary increments.
3. Count is 1 with no recent episode or easing history. Growth must match
   normal CoDel.
4. The workload's oracle falls from 100 to an intermediate value such as 60.
   Recovery memory must not force repeated returns to 100.
5. The workload disappears. Count must eventually return to 1 and all recovery
   memory must expire.
6. Load rises above every recently remembered value. Memory may accelerate the
   initial recovery, but ordinary CoDel growth must continue beyond it.

### 9. Evaluation Order

1. Reject unstable formulas using the deterministic controller model.
2. Tune broad parameter ranges using deterministic queueing workloads.
3. Validate the surviving ranges under noisy workloads and multiple seeds.
4. Compare the best candidates against the current implementation using
   identical traces.
5. Only after selecting a formula, add production unit tests that reproduce
   the model's required transition sequences.

## Deterministic Harness

The first modeling phase is implemented in:

```text
go/vt/vttablet/tabletserver/loadshed/model
```

Run the stationary noisy-count model with:

```sh
go run ./go/vt/vttablet/tabletserver/loadshed/model \
  -profile stationary \
  -required-count 100 \
  -noise-stddev 8
```

Run a high-to-low-to-high workload with:

```sh
go run ./go/vt/vttablet/tabletserver/loadshed/model \
  -profile down-up \
  -required-count 100 \
  -step-count 60 \
  -noise-stddev 8
```

Sweep required counts under IID and correlated noise with:

```sh
go run ./go/vt/vttablet/tabletserver/loadshed/model \
  -profile stationary \
  -required-counts 5,10,25,50,100,250,500 \
  -noise-fraction 0.08 \
  -noise-models iid,ar1 \
  -noise-correlations 0.5,0.9
```

`-noise-fraction` scales the marginal noise standard deviation with the
required count. This keeps the relative observation variance comparable across
the count sweep. For AR(1) noise, the innovation variance is adjusted so each
correlation value retains the requested marginal standard deviation.

The model treats each step as one controller observation. The observed required
count is the workload's underlying required count plus Gaussian noise. An
observation is healthy when the current count meets or exceeds that noisy
requirement.

This is intentionally not yet a queueing simulation. Its purpose is to expose
count-update instability, cold-start changes, limit cycles, and recovery
behavior before adding arrival, service, and timer mechanics.

The output reports:

- Count error relative to the underlying required count.
- Under- and over-count components.
- Healthy/unhealthy transition rate.
- Ordinary, recovery, and easing count movement.
- Adaptation time for the down-up profile.

### Initial Results

The initial runs used 20 seeds, 30,000 intervals per seed, an underlying
required count of 100, and Gaussian observation noise with standard deviation
8.

#### Easing quantization

The current easing step is:

```text
step = floor(log_base(count) / base)
step = max(step, 1)
```

The current integer log-base formula has a material quantization boundary near
this count. Bases from 3.0 through 2.55 produced the same count sequence. Base
2.5 was the first tested value that changed the healthy easing step from 1 to
2.

This means that changing the log base does not necessarily produce a small
change in behavior. A range of values may be identical, followed by a boundary
where the easing step changes abruptly. These boundaries move with count, so a
base that is unchanged around count 100 may still behave differently at a
higher or lower count.

The default sweep is therefore centered around the first observed boundary
rather than treating evenly spaced log bases as evenly spaced controller
strengths.

#### Stationary noisy workload

Selected results:

| Recovery | Base | Retention | Mean count | Mean absolute error | p95 error | Mean under | Mean over | Healthy fraction |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| None | 3.0 | - | 100.017 | 1.806 | 4 | 0.895 | 0.911 | 0.500 |
| None | 2.5 | - | 97.650 | 2.685 | 5 | 2.517 | 0.167 | 0.387 |
| Partial replay | 2.5 | 0.25 | 98.071 | 2.418 | 5 | 2.173 | 0.244 | 0.407 |
| Partial replay | 2.5 | 0.50 | 98.741 | 2.254 | 5 | 1.756 | 0.497 | 0.440 |
| Partial replay | 2.5 | 0.75 | 99.175 | 2.281 | 5 | 1.553 | 0.728 | 0.460 |
| None | 2.25 | - | 96.358 | 3.882 | 8 | 3.762 | 0.120 | 0.333 |
| Partial replay | 2.25 | 0.75 | 98.640 | 2.683 | 7 | 2.022 | 0.661 | 0.437 |
| Easing debt | 3.0 | - | 103.657 | 3.892 | 8 | 0.117 | 3.775 | 0.667 |

Moving from base 3.0 to 2.5 without recovery increased mean absolute count
error by 48.7%. The error was strongly directional: mean under-count increased
from 0.895 to 2.517, an increase of 181.2%, while mean over-count fell from
0.911 to 0.167. The controller spent more time below the underlying required
count and classified only 38.7% of intervals as healthy.

Partial replay moved the distribution back toward the required count:

- Retention 0.25 reduced mean absolute error by 9.9% relative to base 2.5
  without recovery.
- Retention 0.50 reduced mean absolute error by 16.1% and under-count by 30.2%.
- Retention 0.75 reduced under-count by 38.3%, but increased over-count enough
  that its total error was slightly worse than retention 0.50.

This gives retention a visible count-bias tradeoff. Higher retention spends
less time below the synthetic required count and preserves more count after the
workload observation becomes healthy. The deterministic model cannot establish
which bias produces the better latency and error balance. In this workload,
retention 0.50 had the lowest total count error while retention 0.75 had the
lowest under-count.

The same pattern became more pronounced at base 2.25. Partial replay with
retention 0.75 reduced mean absolute error by 30.9% and under-count by 46.3%
relative to base 2.25 without recovery. It still remained worse than the base
3.0 baseline, showing that recovery compensated for only part of the stronger
easing.

#### Count movement

The steady-state count-movement totals explain how each strategy produced its
bias:

| Recovery | Base | Retention | Ordinary increase/1k | Recovery increase/1k | Easing decrease/1k |
|---|---:|---:|---:|---:|---:|
| None | 3.0 | - | 500.0 | 0.0 | 500.0 |
| None | 2.5 | - | 613.0 | 0.0 | 613.0 |
| Partial replay | 2.5 | 0.50 | 560.3 | 213.0 | 773.3 |
| Easing debt | 3.0 | - | 333.3 | 333.3 | 666.7 |

The base 3.0 controller balanced one ordinary count increment against one
easing decrement in roughly half the intervals. Base 2.5 required more
ordinary increments because some healthy intervals removed two count units.

With partial replay at retention 0.50, recovery supplied about 28% of all
upward count movement. This reduced the number of ordinary increments required
to regain count, while retaining a net downward bias.

Easing debt produced a different balance. Every restored count unit was added
on top of the normal increment associated with the unhealthy observation. That
made recovery very fast, but shifted the stationary mean count 3.657 above the
required value. The high over-count is evidence that unconditional debt
restoration is too strong in this model without damping.

#### High-to-low-to-high workload

The workload used a required count of 100, stepped down to 60 for the middle
third of the run, and returned to 100 for the final third. Adaptation time is
the number of intervals required to enter a 10% band around the new required
count.

| Recovery | Base | Retention | Mean absolute error | Release steps | Recovery steps |
|---|---:|---:|---:|---:|---:|
| None | 3.0 | - | 1.863 | 36.10 | 30.30 |
| None | 2.5 | - | 2.410 | 33.90 | 30.50 |
| Partial replay | 2.5 | 0.50 | 2.246 | 34.35 | 29.30 |
| None | 2.25 | - | 3.923 | 15.75 | 35.10 |
| Partial replay | 2.25 | 0.75 | 2.727 | 20.15 | 31.15 |
| Easing debt | 2.25 | - | 3.493 | 19.55 | 9.80 |

Base 2.5 without recovery released count only 6.1% faster than base 3.0 and
recovered at essentially the same speed. Partial replay with retention 0.50
made recovery 3.9% faster than base 2.5 without recovery, at the cost of a
slightly slower release.

Base 2.25 exposed the adaptation tradeoff more clearly:

- Release was 56.4% faster than the base 3.0 baseline.
- Recovery was 15.8% slower than the base 3.0 baseline.
- Partial replay with retention 0.75 made recovery 11.3% faster than base 2.25
  without recovery.
- That partial-replay configuration still released 44.2% faster than the base
  3.0 baseline.

Easing debt recovered 72.1% faster than base 2.25 without recovery and 67.7%
faster than the base 3.0 baseline. Its stationary results show why recovery
time cannot be considered alone: most of that speed came with a substantially
different stationary count distribution.

#### Required-count sweep

The next sweep used required counts from 5 through 500 and noise standard
deviation equal to 8% of required count. It compared base 3.0, base 2.5, and
partial replay with retention 0.50.

IID results:

| Required count | Base 3.0 error | Base 2.5 error | Base 2.5 with replay | Replay change versus base 2.5 |
|---:|---:|---:|---:|---:|
| 5 | 0.506 | 0.506 | 0.532 | +5.1% |
| 10 | 0.607 | 0.607 | 0.651 | +7.2% |
| 25 | 0.913 | 0.913 | 0.993 | +8.8% |
| 50 | 1.282 | 1.282 | 1.424 | +11.1% |
| 100 | 1.806 | 2.685 | 2.254 | -16.1% |
| 250 | 2.859 | 8.869 | 5.224 | -41.1% |
| 500 | 4.044 | 17.399 | 9.282 | -46.7% |

Base 2.5 and base 3.0 were identical through required count 50 because both
produced an easing step of 1 in that range. The base 2.5 step becomes 2 at:

```text
count >= 2.5^(2*2.5) = 97.65625
```

This explains the abrupt change between required counts 50 and 100. Partial
replay was harmful below the boundary because there was no additional easing
to recover from. It added upward count movement to a controller that already
matched the baseline easing behavior.

Above the boundary, partial replay recovered an increasing fraction of the
error introduced by base 2.5. At counts 250 and 500 it nearly halved mean
absolute error relative to base 2.5 without recovery. It did not recover the
stability of base 3.0: at count 500, replay error was still 130% above the
current baseline.

This suggests that recovery should be conditioned on count removed beyond the
baseline easing step. Applying episode replay whenever recent memory exists is
too broad. A candidate implementation should distinguish ordinary easing from
additional easing introduced by a stronger formula.

#### Correlated-noise sweep

AR(1) noise used correlations 0.5 and 0.9 with the same 8% marginal standard
deviation as the IID runs.

Selected results:

| Required count | Correlation | Base 3.0 error | Base 2.5 error | Base 2.5 with replay | Replay change versus base 2.5 |
|---:|---:|---:|---:|---:|---:|
| 100 | 0.5 | 2.613 | 3.085 | 2.837 | -8.0% |
| 100 | 0.9 | 4.981 | 4.931 | 4.780 | -3.1% |
| 250 | 0.5 | 4.217 | 9.101 | 5.640 | -38.0% |
| 250 | 0.9 | 9.117 | 11.850 | 10.491 | -11.5% |
| 500 | 0.5 | 5.990 | 17.372 | 8.563 | -50.7% |
| 500 | 0.9 | 13.529 | 20.450 | 16.901 | -17.4% |

Correlation increased the duration of excursions on either side of the
required count. For example, base 3.0 mean absolute error at count 100 rose
from 1.806 under IID noise to 2.613 at correlation 0.5 and 4.981 at correlation
0.9.

Partial replay continued to help base 2.5 after its easing step crossed the
quantization boundary, but its relative benefit generally shrank under high
correlation. Longer runs of healthy or unhealthy observations allowed count to
move farther before an episode transition supplied a new replay value.

At count 500 and correlation 0.5, partial replay reduced base 2.5 error by
50.7%. At correlation 0.9, the reduction was only 17.4%. This indicates that
retention and memory age should be evaluated against episode duration, not just
the marginal noise variance.

Partial replay with base 3.0 generally shifted stationary count upward without
improving total error. This reinforces that replay should compensate for
identified excess easing rather than act as unconditional count memory.

#### Excess-easing replay

Two variants were added to connect replay explicitly to easing beyond the base
3.0 behavior.

The first caps replay at the accumulated excess easing:

```text
excess = candidateEasingStep - base3EasingStep
recovery = min(partialReplayRecovery, accumulatedExcess)
```

The second uses excess easing as a gate rather than a cap:

```text
if accumulatedExcess > 0:
    recovery = partialReplayRecovery
else:
    recovery = 0
```

Both variants clear accumulated excess when the next unhealthy episode starts.
They are therefore identical to no recovery when the candidate and base 3.0
formulas produce the same easing step.

The capped variant removed the low-count regression from unconditional replay,
but usually recovered too little to match its stationary benefit. At required
count 100 with IID noise, base 2.5 and retention 0.75 produced mean absolute
error 2.289, compared with 2.254 for unconditional replay and 1.806 for base
3.0.

The gated variant was then tested at bases whose first two-unit easing boundary
was immediately above the required count:

```text
base 2.74 boundary: 250.54
base 2.91 boundary: 501.02
base 3.00 boundary: 729.00
```

This causes ordinary one-unit easing below the boundary and occasional
two-unit easing during upward excursions. A 100-seed sweep with retention 0.10
gave:

| Required count | Noise | Base 3.0 error | Gated replay error | Base 3.0 under/over | Gated under/over |
|---:|---|---:|---:|---:|---:|
| 250 | IID | 2.832 | 2.460 | 1.415 / 1.417 | 2.034 / 0.427 |
| 250 | AR(1), 0.5 | 4.219 | 3.659 | 2.101 / 2.118 | 2.514 / 1.144 |
| 250 | AR(1), 0.9 | 9.113 | 8.582 | 4.535 / 4.578 | 5.124 / 3.458 |
| 500 | IID | 3.994 | 3.254 | 1.985 / 2.009 | 2.694 / 0.560 |
| 500 | AR(1), 0.5 | 6.004 | 5.013 | 2.992 / 3.012 | 3.526 / 1.488 |
| 500 | AR(1), 0.9 | 13.602 | 12.753 | 6.773 / 6.829 | 8.121 / 4.633 |

Total absolute error improved by 5.8% to 18.5%, depending on count and noise
correlation. The improvement came from suppressing over-count excursions.
Under-count increased in every case, by 13.0% to 43.7%. This identifies a
distinct controller bias, but does not establish whether latency would
increase. That requires a model in which count affects queue sojourn, shedding,
and completed throughput.

The corresponding down-up transitions showed little adaptation benefit:

| Required count change | Base 3.0 release/recovery | Gated release/recovery |
|---|---:|---:|
| 250 → 150 → 250 | 91.80 / 76.90 | 90.60 / 76.90 |
| 500 → 300 → 500 | 185.70 / 153.90 | 183.85 / 154.15 |

Release improved by about 1%, while recovery was effectively unchanged. The
lower aggregate count error in these runs therefore does not come from
materially faster count adaptation. The stationary behavior still makes these
useful candidates for queue-level testing.

The benefit was also narrow around the selected quantization boundary. With
base 2.91 at required count 550, gated replay increased IID mean absolute error
from 4.242 to 14.950. Once the workload remains above the new boundary,
two-unit easing occurs repeatedly and replay does not remove the resulting
downward bias.

These results show that a sweep setting can beat base 3.0 for symmetric count
error near a deliberately aligned boundary. No tested static base provides a
general count-error improvement across the full range. The local gains depend
on placing a discontinuous easing threshold at the workload's equilibrium and
consistently increase under-count. Both the lower total error and the changed
bias are reasons to carry the configurations into a realistic model, not
reasons to accept or reject them in isolation.

#### Smooth fractional easing

Fractional easing preserves the base 3.0 integer step and accumulates only the
continuous difference between a candidate formula and base 3.0:

```text
fractionalExtra =
    strength * max(candidateRawStep - base3RawStep, 0)

credit += fractionalExtra
extra = floor(credit)
credit -= extra

count -= base3IntegerStep + extra
```

Unused credit is multiplied by a configurable decay factor on each unhealthy
observation. This makes extra easing accumulate during a sustained healthy run
while allowing alternating healthy and unhealthy observations to dissipate it.
Only extra units actually removed from count are recorded for gated replay.
Cold-start growth and a fractional strength of zero exactly match base 3.0.

The first sweep showed that credit decay of 0.9 or below made low strengths
nearly indistinguishable from base 3.0 under stationary alternating
observations. During a sustained healthy phase after a load decrease, the same
configuration accumulated enough credit to release count faster.

A gentle configuration used candidate base 2.5, strength 0.10, credit decay
0.90, and no recovery:

| Required count change | Base 3.0 error | Fractional error | Base 3.0 release/recovery | Fractional release/recovery |
|---|---:|---:|---:|---:|
| 100 → 60 → 100 | 1.863 | 1.865 | 36.10 / 30.30 | 34.55 / 30.25 |
| 250 → 150 → 250 | 3.228 | 3.301 | 91.80 / 76.90 | 85.40 / 77.15 |
| 500 → 300 → 500 | 5.573 | 5.662 | 185.70 / 153.90 | 170.65 / 154.50 |

Release improved by 4.3%, 7.0%, and 8.1% as count increased. Recovery time was
effectively unchanged, and aggregate count error remained within 2.3% of the
base 3.0 result.

A moderate configuration used strength 0.25, decay 0.90, gated replay, and
retention 0.10:

| Required count change | Base 3.0 error | Fractional error | Base 3.0 release/recovery | Fractional release/recovery |
|---|---:|---:|---:|---:|
| 100 → 60 → 100 | 1.863 | 1.958 | 36.10 / 30.30 | 31.65 / 30.25 |
| 250 → 150 → 250 | 3.228 | 3.377 | 91.80 / 76.90 | 79.50 / 76.00 |
| 500 → 300 → 500 | 5.573 | 5.749 | 185.70 / 153.90 | 157.15 / 153.40 |

Release improved by 12.3% to 15.4%, while recovery remained equal or slightly
faster. Aggregate count error increased by 3.2% to 5.1% in these transitions.
Across the stationary IID and AR(1) sweep, its mean absolute count error stayed
within 8.5% of base 3.0.

For comparison, integer base 2.5 with retention 0.50 released much faster at
counts 250 and 500, but its transition count error was roughly twice the
fractional configuration's. Fractional easing therefore supplies a continuum:
strength and credit persistence can increase release speed without crossing an
integer easing boundary on every healthy observation.

Both fractional configurations are candidates for queue-level testing. The
gentle version isolates whether a modest release improvement is useful in
practice. The moderate gated-replay version tests whether larger release gains
translate into lower post-load-decrease shedding without harming latency when
load returns.

#### Interpretation

These results are directional rather than production predictions. They show
that slightly crossing the formula's quantization boundary can create a strong
downward count bias. Partial replay recovers part of that bias while adding
some over-count. Easing debt recovers very quickly after the load returns, but
its stationary over-count is substantially worse.

The deterministic model does not simulate queue depth, sojourn time, control-law
pacing, multiple overdue drops, or the exponent. Under-count and over-count
describe how a candidate differs from the synthetic equilibrium. Neither is a
direct latency, shedding, error-rate, or throughput measurement.

The useful conclusions from this phase are:

1. The first increase in easing strength is discontinuous, not gradual.
2. Partial replay provides a tunable under-count versus over-count tradeoff.
3. Retention around 0.50 is the strongest initial candidate near the first
   easing boundary.
4. Retention 0.75 becomes more useful as easing grows stronger, but does not
   fully recover baseline stability.
5. Undamped easing debt should remain a fast-recovery comparison point in the
   realistic model.
6. The next phase must test several required-count levels because easing
   quantization boundaries depend on count.
7. Recovery should be activated by excess easing, not merely by the existence
   of recent episode memory.
8. High correlation reduces the value of episode-boundary recovery and may
   require retention or memory decay to account for episode duration.
9. Gating full replay on excess easing avoids low-count replay regressions, but
   the observed benefit remains local to a quantization boundary.
10. Static-base tuning cannot provide a smooth increase in easing strength;
    fractional excess easing supplies that missing control.
11. Fractional credit decay makes easing context-sensitive: little extra easing
    occurs during noisy alternation, while sustained health accumulates faster
    release.

#### Candidates for realistic testing

The deterministic benchmark is a candidate generator. It is sufficient to
identify meaningfully different controller behaviors, but not to rank their
production value. The next environment should retain:

1. Base 3.0 without recovery as the current baseline.
2. Base 2.5 with partial replay at retention 0.50 as the moderate
   stronger-easing candidate.
3. Base 2.25 with partial replay at retention 0.75 as the faster-release
   candidate.
4. Base 2.74 with gated replay at retention 0.10 as the count-250
   boundary-aligned candidate.
5. Base 2.91 with gated replay at retention 0.10 as the count-500
   boundary-aligned candidate.
6. Fractional base 2.5 with strength 0.10, decay 0.90, and no recovery as the
   gentle smooth-easing candidate.
7. Fractional base 2.5 with strength 0.25, decay 0.90, gated replay, and
   retention 0.10 as the moderate smooth-easing candidate.
8. Easing debt as a fast-recovery comparison point.

These candidates span different count biases and adaptation speeds. They
should be compared using actual queue latency, shed fraction, completed
throughput, and time to recover after load changes. Count error remains useful
for explaining the resulting behavior, but should not determine the winner.

## Initial Hypothesis

Partial replay is more likely than reversible easing debt to settle at an
intermediate value because the recovery destination changes with each observed
episode increase. Reversible easing debt remains useful as a baseline because
it isolates the benefit of recovering only controller-induced count reduction.

The primary comparison should be whether partial replay can reduce adaptation
time without increasing stationary effective drop-rate variance or excess
shedding relative to the current slow-easing implementation.
