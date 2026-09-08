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

package loadshed

import (
	"math"
	"time"
)

var initialTargetShadowCandidates = durationNanos(
	5*time.Millisecond,
	10*time.Millisecond,
	20*time.Millisecond,
	40*time.Millisecond,
	80*time.Millisecond,
	160*time.Millisecond,
	320*time.Millisecond,
	640*time.Millisecond,
)

const initialTargetShadowIntervalRatio = int64(20)

var initialTargetShadowMissNs = initialTargetShadowCandidates[len(initialTargetShadowCandidates)-1] + 1
var initialTargetShadowMaxIntervalNs = initialTargetShadowCandidates[len(initialTargetShadowCandidates)-1] * initialTargetShadowIntervalRatio

type (
	initialTargetShadowOutcome struct {
		completed        bool
		requiredTargetNs int64
	}

	initialTargetShadowTracker struct {
		active          bool
		waitingForDrain bool
		startedAtNs     int64
		hits            uint8
	}
)

func (t *initialTargetShadowTracker) start(nowNs int64) bool {
	if t.active || t.waitingForDrain || nowNs > math.MaxInt64-initialTargetShadowMaxIntervalNs {
		return false
	}

	t.active = true
	t.startedAtNs = nowNs
	t.hits = 0
	return true
}

func (t *initialTargetShadowTracker) observe(
	nowNs int64,
	sojournNs *int64,
	drained bool,
) initialTargetShadowOutcome {
	if t.waitingForDrain {
		if drained {
			t.waitingForDrain = false
		}
		return initialTargetShadowOutcome{}
	}
	if !t.active {
		return initialTargetShadowOutcome{}
	}

	if nowNs >= t.startedAtNs+initialTargetShadowMaxIntervalNs {
		return t.complete(!drained)
	}

	if sojournNs != nil {
		for i, targetNs := range initialTargetShadowCandidates {
			if nowNs < t.deadlineNs(targetNs) && *sojournNs < targetNs {
				t.hits |= 1 << i
			}
		}
	}

	if drained {
		for i, targetNs := range initialTargetShadowCandidates {
			if nowNs < t.deadlineNs(targetNs) {
				t.hits |= 1 << i
			}
		}
		return t.complete(false)
	}

	if t.hits&1 != 0 {
		return t.complete(true)
	}
	return initialTargetShadowOutcome{}
}

func (t *initialTargetShadowTracker) deadlineNs(targetNs int64) int64 {
	return t.startedAtNs + targetNs*initialTargetShadowIntervalRatio
}

func (t *initialTargetShadowTracker) complete(waitingForDrain bool) initialTargetShadowOutcome {
	requiredTargetNs := initialTargetShadowMissNs
	for i, targetNs := range initialTargetShadowCandidates {
		if t.hits&(1<<i) != 0 {
			requiredTargetNs = targetNs
			break
		}
	}
	t.reset(waitingForDrain)
	return initialTargetShadowOutcome{
		completed:        true,
		requiredTargetNs: requiredTargetNs,
	}
}

func (t *initialTargetShadowTracker) reset(waitingForDrain bool) {
	t.active = false
	t.waitingForDrain = waitingForDrain
	t.startedAtNs = 0
	t.hits = 0
}
