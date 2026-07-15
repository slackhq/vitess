/*
Copyright 2020 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"math"
	"net/http"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/safehtml/template"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

var (
	debugEnvHeader = []byte(`
	<thead><tr>
		<th>Variable Name</th>
		<th>Value</th>
		<th>Action</th>
	</tr></thead>
	`)
	debugEnvRow = template.Must(template.New("debugenv").Parse(`
	<tr><form method="POST">
		<td>{{.Name}}</td>
		<td>
			<input type="hidden" name="varname" value="{{.Name}}"></input>
			<input type="text" name="value" value="{{.Value}}"></input>
		</td>
		<td><input type="submit" name="Action" value="Modify"></input></td>
	</form></tr>
	`))
)

type envValue struct {
	Name  string
	Value string
}

// this cannot be an anonymous function within debugEnvHandler because those kinds
// of functions cannot (currently) have type params.
func addVar[T any](vars []envValue, name string, f func() T) []envValue {
	return append(vars, envValue{
		Name:  name,
		Value: fmt.Sprintf("%v", f()),
	})
}

func debugEnvHandler(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
		acl.SendError(w, err)
		return
	}

	switch r.Method {
	case http.MethodPost:
		handlePost(tsv, w, r)
	case http.MethodGet:
		handleGet(tsv, w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handlePost(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	varname := r.FormValue("varname")
	value := r.FormValue("value")

	var msg string
	if varname == "" || value == "" {
		http.Error(w, "Missing varname or value", http.StatusBadRequest)
		return
	}

	setIntVal := func(f func(int)) error {
		ival, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int value for %v: %v", varname, err)
		}
		f(ival)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setIntValCtx := func(f func(context.Context, int) error) error {
		ival, err := strconv.Atoi(value)
		if err == nil {
			err = f(r.Context(), ival)
		}
		if err != nil {
			return fmt.Errorf("failed setting value for %v: %v", varname, err)
		}
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setInt64Val := func(f func(int64)) error {
		ival, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid int64 value for %v: %v", varname, err)
		}
		f(ival)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setDurationVal := func(f func(time.Duration)) error {
		durationVal, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration value for %v: %v", varname, err)
		}
		f(durationVal)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setFloat64Val := func(f func(float64)) error {
		fval, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid float64 value for %v: %v", varname, err)
		}
		f(fval)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setStringVal := func(f func(string) error) error {
		if err := f(value); err != nil {
			return fmt.Errorf("invalid value for %v: %v", varname, err)
		}
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setBoolVal := func(f func(bool)) error {
		bval, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool value for %v: %v", varname, err)
		}
		f(bval)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	var err error
	switch varname {
	case "ReadPoolSize":
		err = setIntValCtx(tsv.SetPoolSize)
	case "StreamPoolSize":
		err = setIntValCtx(tsv.SetStreamPoolSize)
	case "TransactionPoolSize":
		err = setIntValCtx(tsv.SetTxPoolSize)
	case "MaxResultSize":
		err = setIntVal(tsv.SetMaxResultSize)
	case "WarnResultSize":
		err = setIntVal(tsv.SetWarnResultSize)
	case "RowStreamerMaxInnoDBTrxHistLen":
		err = setInt64Val(func(val int64) { tsv.Config().RowStreamer.MaxInnoDBTrxHistLen = val })
	case "RowStreamerMaxMySQLReplLagSecs":
		err = setInt64Val(func(val int64) { tsv.Config().RowStreamer.MaxMySQLReplLagSecs = val })
	case "UnhealthyThreshold":
		err = setDurationVal(func(d time.Duration) { tsv.Config().Healthcheck.UnhealthyThreshold = d })
	case "ThrottleMetricThreshold":
		err = setFloat64Val(tsv.SetThrottleMetricThreshold)
	case "LoadshedTarget":
		err = setDurationVal(func(d time.Duration) { tsv.Config().LoadshedTarget = d })
	case "LoadshedInterval":
		err = setDurationVal(func(d time.Duration) { tsv.Config().LoadshedInterval = d })
	case "LoadshedExponent":
		err = setFloat64Val(func(v float64) { tsv.Config().LoadshedExponent = v })
	case "LoadshedTrigger":
		err = setDurationVal(func(d time.Duration) { tsv.Config().LoadshedTrigger = d })
	case "LoadshedGraceCount":
		err = setIntVal(func(v int) { tsv.Config().LoadshedGraceCount = v })
	case "LoadshedMinDropDelay":
		err = setDurationVal(func(d time.Duration) { tsv.Config().LoadshedMinDropDelay = d })
	case "LoadshedPerCPUIntake":
		err = setBoolVal(func(b bool) { tsv.Config().LoadshedPerCPUIntake = b })
	case "LoadshedKeepDroppableFloor":
		err = setIntVal(func(v int) { tsv.Config().LoadshedKeepDroppableFloor = v })
	case "LoadshedMaxDropsPerFire":
		err = setIntVal(func(v int) { tsv.Config().LoadshedMaxDropsPerFire = v })
	case "LoadshedYieldOnGrant":
		err = setBoolVal(func(b bool) { tsv.Config().LoadshedYieldOnGrant = b })
	case "LoadshedYieldOnDrop":
		err = setBoolVal(func(b bool) { tsv.Config().LoadshedYieldOnDrop = b })
	case "LoadshedUndroppableSchemas":
		err = setStringVal(func(v string) error {
			var schemas []string
			for _, s := range strings.Split(v, ",") {
				if s = strings.TrimSpace(s); s != "" {
					schemas = append(schemas, s)
				}
			}
			tsv.Config().LoadshedUndroppableSchemas = schemas
			return nil
		})
	case "LoadshedDropMode":
		err = setStringVal(func(v string) error {
			if _, perr := loadshed.ParseDropMode(v); perr != nil {
				return perr
			}
			tsv.Config().LoadshedDropMode = v
			return nil
		})
	case "GOGC":
		// Process-global Go GC target percent. >0 sets the heap-growth target;
		// values <= 0 are rejected here to avoid accidentally disabling GC via the
		// UI (use GOMEMLIMIT for a soft cap instead).
		err = setStringVal(func(v string) error {
			pct, perr := strconv.Atoi(v)
			if perr != nil {
				return fmt.Errorf("invalid GOGC %q: %v", v, perr)
			}
			if pct <= 0 {
				return fmt.Errorf("GOGC must be > 0, got %d", pct)
			}
			debug.SetGCPercent(pct)
			return nil
		})
	case "GOMEMLIMIT":
		// Process-global Go soft memory limit. Accepts a human size (e.g. 13GiB,
		// 500MiB) or a raw byte count; "off" (or 0) restores the default (no soft
		// limit, math.MaxInt64).
		err = setStringVal(func(v string) error {
			var limit int64
			if v == "off" || v == "0" {
				limit = math.MaxInt64
			} else {
				n, perr := humanize.ParseBytes(v)
				if perr != nil {
					return fmt.Errorf("invalid GOMEMLIMIT %q: %v", v, perr)
				}
				limit = int64(n)
			}
			debug.SetMemoryLimit(limit)
			return nil
		})
	case "Consolidator":
		tsv.SetConsolidatorMode(value)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	vars := getVars(tsv)
	sendResponse(r, w, vars, msg)
}

func handleGet(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	vars := getVars(tsv)
	sendResponse(r, w, vars, "")
}

func sendResponse(r *http.Request, w http.ResponseWriter, vars []envValue, msg string) {
	format := r.FormValue("format")
	if format == "json" {
		respondWithJSON(w, vars, msg)
		return
	}
	respondWithHTML(w, vars, msg)
}

func getVars(tsv *TabletServer) []envValue {
	var vars []envValue
	vars = addVar(vars, "ReadPoolSize", tsv.PoolSize)
	vars = addVar(vars, "StreamPoolSize", tsv.StreamPoolSize)
	vars = addVar(vars, "TransactionPoolSize", tsv.TxPoolSize)
	vars = addVar(vars, "MaxResultSize", tsv.MaxResultSize)
	vars = addVar(vars, "WarnResultSize", tsv.WarnResultSize)
	vars = addVar(vars, "RowStreamerMaxInnoDBTrxHistLen", func() int64 { return tsv.Config().RowStreamer.MaxInnoDBTrxHistLen })
	vars = addVar(vars, "RowStreamerMaxMySQLReplLagSecs", func() int64 { return tsv.Config().RowStreamer.MaxMySQLReplLagSecs })
	vars = addVar(vars, "UnhealthyThreshold", func() time.Duration { return tsv.Config().Healthcheck.UnhealthyThreshold })
	vars = addVar(vars, "ThrottleMetricThreshold", tsv.ThrottleMetricThreshold)
	vars = addVar(vars, "LoadshedTarget", func() time.Duration { return tsv.Config().LoadshedTarget })
	vars = addVar(vars, "LoadshedInterval", func() time.Duration { return tsv.Config().LoadshedInterval })
	vars = addVar(vars, "LoadshedExponent", func() float64 { return tsv.Config().LoadshedExponent })
	vars = addVar(vars, "LoadshedDropMode", func() string { return tsv.Config().LoadshedDropMode })
	vars = addVar(vars, "LoadshedTrigger", func() time.Duration { return tsv.Config().LoadshedTrigger })
	vars = addVar(vars, "LoadshedGraceCount", func() int { return tsv.Config().LoadshedGraceCount })
	vars = addVar(vars, "LoadshedMinDropDelay", func() time.Duration { return tsv.Config().LoadshedMinDropDelay })
	vars = addVar(vars, "LoadshedPerCPUIntake", func() bool { return tsv.Config().LoadshedPerCPUIntake })
	vars = addVar(vars, "LoadshedKeepDroppableFloor", func() int { return tsv.Config().LoadshedKeepDroppableFloor })
	vars = addVar(vars, "LoadshedMaxDropsPerFire", func() int { return tsv.Config().LoadshedMaxDropsPerFire })
	vars = addVar(vars, "LoadshedYieldOnGrant", func() bool { return tsv.Config().LoadshedYieldOnGrant })
	vars = addVar(vars, "LoadshedYieldOnDrop", func() bool { return tsv.Config().LoadshedYieldOnDrop })
	vars = append(vars, envValue{
		Name:  "LoadshedUndroppableSchemas",
		Value: strings.Join(tsv.Config().LoadshedUndroppableSchemas, ","),
	})
	vars = append(vars, envValue{
		Name:  "Consolidator",
		Value: tsv.ConsolidatorMode(),
	})

	// Go runtime GC knobs, read live from runtime/metrics so the values reflect
	// whatever the environment (GOGC/GOMEMLIMIT) set at startup plus any /debug/env
	// override. GOMEMLIMIT is rendered as a human size; math.MaxInt64 means "no
	// soft limit" (the default).
	gogc, gomemlimit := readGCSettings()
	vars = addVar(vars, "GOGC", func() int { return gogc })
	memLimitStr := "off"
	if gomemlimit != math.MaxInt64 {
		memLimitStr = humanize.IBytes(uint64(gomemlimit))
	}
	vars = append(vars, envValue{Name: "GOMEMLIMIT", Value: memLimitStr})

	return vars
}

// readGCSettings reads the current Go GC target percent and soft memory limit
// from runtime/metrics. gomemlimit is math.MaxInt64 when no soft limit is set.
func readGCSettings() (gogc int, gomemlimit int64) {
	samples := []metrics.Sample{
		{Name: "/gc/gogc:percent"},
		{Name: "/gc/gomemlimit:bytes"},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		gogc = int(samples[0].Value.Uint64())
	}
	if samples[1].Value.Kind() == metrics.KindUint64 {
		gomemlimit = int64(samples[1].Value.Uint64())
	}
	return gogc, gomemlimit
}

func respondWithJSON(w http.ResponseWriter, vars []envValue, msg string) {
	mvars := make(map[string]string)
	for _, v := range vars {
		mvars[v.Name] = v.Value
	}
	if msg != "" {
		mvars["ResponseMessage"] = msg
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(mvars)
}

func respondWithHTML(w http.ResponseWriter, vars []envValue, msg string) {
	w.Write(gridTable)
	w.Write([]byte("<h3>Internal Variables</h3>\n"))
	if msg != "" {
		fmt.Fprintf(w, "<b>%s</b><br /><br />\n", html.EscapeString(msg))
	}
	w.Write(startTable)
	w.Write(debugEnvHeader)
	for _, v := range vars {
		if err := debugEnvRow.Execute(w, v); err != nil {
			log.Errorf("debugenv: couldn't execute template: %v", err)
		}
	}
	w.Write(endTable)
}
