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

// Package stats is a wrapper for expvar. It additionally
// exports new types that can be used to track performance.
// It also provides a callback hook that allows a program
// to export the variables using methods other than /debug/vars.
// All variables support a String function that
// is expected to return a JSON representation
// of the variable.
// Any function named Add will add the specified
// number to the variable.
// Any function named Counts returns a map of counts
// that can be used by Rates to track rates over time.
package stats

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/utils"
)

var (
	emitStats         bool
	statsEmitPeriod   = 60 * time.Second
	statsBackend      string
	statsBackendInit  = make(chan struct{})
	combineDimensions string
	dropVariables     string
)

// CommonTags is a comma-separated list of common tags for stats backends
var CommonTags []string

func RegisterFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &emitStats, "emit-stats", emitStats, "If set, emit stats to push-based monitoring and stats backends")
	utils.SetFlagDurationVar(fs, &statsEmitPeriod, "stats-emit-period", statsEmitPeriod, "Interval between emitting stats to all registered backends")
	utils.SetFlagStringVar(fs, &statsBackend, "stats-backend", statsBackend, "The name of the registered push-based monitoring/stats backend to use")
	utils.SetFlagStringVar(fs, &combineDimensions, "stats-combine-dimensions", combineDimensions, `List of dimensions to be combined into a single "all" value in exported stats vars`)
	utils.SetFlagStringVar(fs, &dropVariables, "stats-drop-variables", dropVariables, `Variables to be dropped from the list of exported variables.`)
	utils.SetFlagStringSliceVar(fs, &CommonTags, "stats-common-tags", CommonTags, `Comma-separated list of common tags for the stats backend. It provides both label and values. Example: label1:value1,label2:value2`)
}

// StatsAllStr is the consolidated name if a dimension gets combined.
const StatsAllStr = "all"

// NewVarHook is the type of a hook to export variables in a different way
type NewVarHook func(name string, v expvar.Var)

type varGroup struct {
	sync.Mutex
	vars       map[string]expvar.Var
	newVarHook NewVarHook
}

func (vg *varGroup) register(nvh NewVarHook) {
	vg.Lock()
	defer vg.Unlock()
	if vg.newVarHook != nil {
		panic("You've already registered a function")
	}
	if nvh == nil {
		panic("nil not allowed")
	}
	vg.newVarHook = nvh
	// Call hook on existing vars because some might have been
	// created before the call to register
	for k, v := range vg.vars {
		nvh(k, v)
	}
	vg.vars = nil
}

func (vg *varGroup) publish(name string, v expvar.Var) {
	if isVarDropped(name) {
		return
	}
	vg.Lock()
	defer vg.Unlock()

	expvar.Publish(name, v)
	if vg.newVarHook != nil {
		vg.newVarHook(name, v)
	} else {
		vg.vars[name] = v
	}
}

var defaultVarGroup = varGroup{vars: make(map[string]expvar.Var)}

// Register allows you to register a callback function
// that will be called whenever a new stats variable gets
// created. This can be used to build alternate methods
// of exporting stats variables.
func Register(nvh NewVarHook) {
	defaultVarGroup.register(nvh)
}

// Publish is expvar.Publish+hook
func Publish(name string, v expvar.Var) {
	publish(name, v)
}

func pushAll() error {
	backend, ok := pushBackends[statsBackend]
	if !ok {
		return fmt.Errorf("no PushBackend registered with name %s", statsBackend)
	}
	return backend.PushAll()
}

func pushOne(name string, v Variable) error {
	backend, ok := pushBackends[statsBackend]
	if !ok {
		return fmt.Errorf("no PushBackend registered with name %s", statsBackend)
	}
	return backend.PushOne(name, v)
}

// StringMapFuncWithMultiLabels is a multidimensional string map publisher.
//
// Map keys are compound names made with joining multiple strings with '.',
// and are named by corresponding key labels.
//
// Map values are any string, and are named by the value label.
//
// Since the map is returned by the function, we assume it's in the right
// format (meaning each key is of the form 'aaa.bbb.ccc' with as many elements
// as there are in Labels).
//
// Backends which need to provide a numeric value can set a constant value of 1
// (or whatever is appropriate for the backend) for each key-value pair present
// in the map.
type StringMapFuncWithMultiLabels struct {
	StringMapFunc
	help       string
	keyLabels  []string
	valueLabel string
}

// Help returns the descriptive help message.
func (s StringMapFuncWithMultiLabels) Help() string {
	return s.help
}

// KeyLabels returns the list of key labels.
func (s StringMapFuncWithMultiLabels) KeyLabels() []string {
	return s.keyLabels
}

// ValueLabel returns the value label.
func (s StringMapFuncWithMultiLabels) ValueLabel() string {
	return s.valueLabel
}

// NewStringMapFuncWithMultiLabels creates a new StringMapFuncWithMultiLabels,
// mapping to the provided function. The key labels correspond with components
// of map keys. The value label names the map values.
func NewStringMapFuncWithMultiLabels(name, help string, keyLabels []string, valueLabel string, f func() map[string]string) *StringMapFuncWithMultiLabels {
	t := &StringMapFuncWithMultiLabels{
		StringMapFunc: StringMapFunc(f),
		help:          help,
		keyLabels:     keyLabels,
		valueLabel:    valueLabel,
	}

	if name != "" {
		publish(name, t)
	}

	return t
}

func publish(name string, v expvar.Var) {
	defaultVarGroup.publish(name, v)
}

// PushBackend is an interface for any stats/metrics backend that requires data
// to be pushed to it. It's used to support push-based metrics backends, as expvar
// by default only supports pull-based ones.
type PushBackend interface {
	// PushAll pushes all stats from expvar to the backend.
	PushAll() error
	// PushOne pushes a single stat from expvar to the backend.
	PushOne(name string, v Variable) error
}

var pushBackends = make(map[string]PushBackend)
var pushBackendsLock sync.Mutex
var once sync.Once

func AwaitBackend(ctx context.Context) error {
	if statsBackend == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-statsBackendInit:
		return nil
	}
}

// RegisterPushBackend allows modules to register PushBackend implementations.
// Should be called on init().
func RegisterPushBackend(name string, backend PushBackend) {
	pushBackendsLock.Lock()
	defer pushBackendsLock.Unlock()
	if _, ok := pushBackends[name]; ok {
		log.Fatalf("PushBackend %s already exists; can't register the same name multiple times", name)
	}
	pushBackends[name] = backend
	if name == statsBackend {
		close(statsBackendInit)
	}
	if emitStats {
		// Start a single goroutine to emit stats periodically
		once.Do(func() {
			go emitToBackend(&statsEmitPeriod)
		})
	}
}

// emitToBackend does a periodic emit to the selected PushBackend. If a push fails,
// it will be logged as a warning (but things will otherwise proceed as normal).
func emitToBackend(emitPeriod *time.Duration) {
	ticker := time.NewTicker(*emitPeriod)
	defer ticker.Stop()
	for range ticker.C {
		if err := pushAll(); err != nil {
			// TODO(aaijazi): This might cause log spam...
			log.Warningf("Pushing stats to backend %v failed: %v", statsBackend, err)
		}
	}
}

// FloatFunc converts a function that returns
// a float64 as an expvar.
type FloatFunc func() float64

// Help returns the help string (undefined currently)
func (f FloatFunc) Help() string {
	return "help"
}

// String is the implementation of expvar.var
func (f FloatFunc) String() string {
	return strconv.FormatFloat(f(), 'g', -1, 64)
}

// String is expvar.String+Get+hook
type String struct {
	mu sync.Mutex
	s  string
}

// NewString returns a new String
func NewString(name string) *String {
	v := new(String)
	publish(name, v)
	return v
}

// Set sets the value
func (v *String) Set(value string) {
	v.mu.Lock()
	v.s = value
	v.mu.Unlock()
}

// Get returns the value
func (v *String) Get() string {
	v.mu.Lock()
	s := v.s
	v.mu.Unlock()
	return s
}

// String is the implementation of expvar.var
func (v *String) String() string {
	return strconv.Quote(v.Get())
}

// StringFunc converts a function that returns
// an string as an expvar.
type StringFunc func() string

// String is the implementation of expvar.var
func (f StringFunc) String() string {
	return strconv.Quote(f())
}

// JSONFunc is the public type for a single function that returns json directly.
type JSONFunc func() string

// String is the implementation of expvar.var
func (f JSONFunc) String() string {
	return f()
}

// PublishJSONFunc publishes any function that returns
// a JSON string as a variable. The string is sent to
// expvar as is.
func PublishJSONFunc(name string, f func() string) {
	publish(name, JSONFunc(f))
}

// StringMapFunc is the function equivalent of StringMap
type StringMapFunc func() map[string]string

// String is used by expvar.
func (f StringMapFunc) String() string {
	m := f()
	if m == nil {
		return "{}"
	}
	return stringMapToString(m)
}

func stringMapToString(m map[string]string) string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k, v := range m {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "\"%v\": %v", k, strconv.Quote(v))
	}
	fmt.Fprintf(b, "}")
	return b.String()
}

var (
	varsMu             sync.Mutex
	combinedDimensions map[string]bool
	droppedVars        map[string]bool
)

// IsDimensionCombined returns true if the specified dimension should be combined.
func IsDimensionCombined(name string) bool {
	varsMu.Lock()
	defer varsMu.Unlock()

	if combinedDimensions == nil {
		dims := strings.Split(combineDimensions, ",")
		combinedDimensions = make(map[string]bool, len(dims))
		for _, dim := range dims {
			if dim == "" {
				continue
			}
			combinedDimensions[dim] = true
		}
	}
	return combinedDimensions[name]
}

// safeJoinLabels joins the label values with ".", but first replaces any existing
// "." characters in the labels with the proper replacement, to avoid issues parsing
// them apart later. The function also replaces specific label values with "all"
// if a dimenstion is marked as true in combinedLabels.
func safeJoinLabels(labels []string, combinedLabels []bool) string {
	// fast path that potentially requires 0 allocations
	switch len(labels) {
	case 0:
		return ""
	case 1:
		if combinedLabels == nil || !combinedLabels[0] {
			return safeLabel(labels[0])
		}
		return StatsAllStr
	}

	var b strings.Builder
	size := len(labels) - 1 // number of separators
	for idx, label := range labels {
		if combinedLabels != nil && combinedLabels[idx] {
			size += len(StatsAllStr)
		} else {
			size += len(label)
		}
	}
	b.Grow(size)

	for idx, label := range labels {
		if idx > 0 {
			b.WriteByte('.')
		}
		if combinedLabels != nil && combinedLabels[idx] {
			b.WriteString(StatsAllStr)
		} else {
			appendSafeLabel(&b, label)
		}
	}
	return b.String()
}

// appendSafeLabel is a more efficient version equivalent
// to strings.ReplaceAll(label, ".", "_"), but appends into
// a strings.Builder.
func appendSafeLabel(b *strings.Builder, label string) {
	// first quickly check if there are any periods to be replaced
	found := false
	for i := 0; i < len(label); i++ {
		if label[i] == '.' {
			found = true
			break
		}
	}
	// if there are none, we can just write the label as-is into the
	// Builder.
	if !found {
		b.WriteString(label)
		return
	}

	for i := 0; i < len(label); i++ {
		if label[i] == '.' {
			b.WriteByte('_')
		} else {
			b.WriteByte(label[i])
		}
	}
}

func safeLabel(label string) string {
	// XXX: strings.ReplaceAll is optimal in the case where '.' does not
	// exist in the label name, and will return the string as-is without
	// allocations. So if we are working with a single label, it's preferrable
	// over appendSafeLabel, since appendSafeLabel is required to allocate
	// into a strings.Builder.
	return strings.ReplaceAll(label, ".", "_")
}

func isVarDropped(name string) bool {
	varsMu.Lock()
	defer varsMu.Unlock()

	if droppedVars == nil {
		dims := strings.Split(dropVariables, ",")
		droppedVars = make(map[string]bool, len(dims))
		for _, dim := range dims {
			if dim == "" {
				continue
			}
			droppedVars[dim] = true
		}
	}
	return droppedVars[name]
}

// ParseCommonTags parses a comma-separated string into map of tags
// If you want to global service values like host, service name, git revision, etc,
// this is the place to do it.
func ParseCommonTags(tagMapString []string) map[string]string {
	tags := make(map[string]string)
	for _, input := range tagMapString {
		if strings.Contains(input, ":") {
			tag := strings.Split(input, ":")
			tags[strings.TrimSpace(tag[0])] = strings.TrimSpace(tag[1])
		}
	}
	return tags
}
