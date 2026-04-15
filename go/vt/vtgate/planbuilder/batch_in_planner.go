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

package planbuilder

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// maybeBatchIN checks whether the primitive tree contains a Route with
// non-vindex IN-clause predicates. If so, it wraps the root primitive with
// a BatchIN that re-executes the sub-tree per batch and merges results.
// The BatchIN short-circuits at execution time when the batch size flag is
// disabled (0) or when no bind variable tuples exceed the threshold.
func maybeBatchIN(prim engine.Primitive) engine.Primitive {
	if !hasNonVindexIN(prim) {
		return prim
	}

	batch := &engine.BatchIN{
		Input: prim,
	}

	// Populate merge descriptor from the primitive tree.
	populateBatchINDescriptor(prim, batch)

	return batch
}

// hasNonVindexIN walks the engine primitive tree looking for Route primitives
// whose SELECT query AST contains IN comparisons with non-vindex ListArg bind
// variables (those not prefixed with "__vals").
func hasNonVindexIN(prim engine.Primitive) bool {
	found := false
	engine.Visit(prim, func(node engine.Primitive) {
		if found {
			return
		}
		route, ok := node.(*engine.Route)
		if !ok || route.QueryStatement == nil {
			return
		}
		// Only consider SELECT queries — DML plans should not be batched.
		if _, isSelect := route.QueryStatement.(*sqlparser.Select); !isSelect {
			return
		}
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			cmp, ok := node.(*sqlparser.ComparisonExpr)
			if !ok {
				return true, nil
			}
			if cmp.Operator != sqlparser.InOp {
				return true, nil
			}
			listArg, ok := cmp.Right.(sqlparser.ListArg)
			if !ok {
				return true, nil
			}
			if !strings.HasPrefix(string(listArg), "__vals") {
				found = true
				return false, nil
			}
			return true, nil
		}, route.QueryStatement)
	})
	return found
}

// populateBatchINDescriptor extracts ORDER BY and LIMIT information from the
// primitive tree so that BatchIN can re-sort and re-truncate merged results.
func populateBatchINDescriptor(prim engine.Primitive, batch *engine.BatchIN) {
	engine.Visit(prim, func(node engine.Primitive) {
		switch p := node.(type) {
		case *engine.Route:
			if len(batch.OrderBy) == 0 && len(p.OrderBy) > 0 {
				batch.OrderBy = p.OrderBy
			}
		case *engine.MemorySort:
			if len(batch.OrderBy) == 0 && len(p.OrderBy) > 0 {
				batch.OrderBy = p.OrderBy
			}
		}
	})
}
