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

package sqlparser

const TruncationText = "[TRUNCATED]"

// GetTruncateErrLen is a function used to read the value of truncateErrLen
func (p *Parser) GetTruncateErrLen() int {
	return p.truncateErrLen
}

func TruncateQuery(query string, max int) string {
	sql, comments := SplitMarginComments(query)

	if max == 0 || len(sql) <= max {
		return comments.Leading + sql + comments.Trailing
	}

	// The marker " [TRUNCATED] " needs at least len(TruncationText)+2 bytes (spaces on both sides).
	minLen := len(TruncationText) + 2
	if max < minLen {
		max = minLen
	}

	// After enforcing the minimum, the SQL may now fit without truncation.
	if len(sql) <= max {
		return comments.Leading + sql + comments.Trailing
	}

	return comments.Leading + truncateMiddle(sql, max) + comments.Trailing
}

// truncateMiddle removes content from the middle of s to fit within max bytes,
// preserving both the beginning (query structure, table names) and the end
// (WHERE clauses, trailing context). The removed section is replaced with
// " [TRUNCATED] ".
//
// The split is 2/3 prefix, 1/3 suffix. This ratio favors the beginning of the
// query (which contains SELECT/INSERT/UPDATE keywords and table names) while
// still preserving meaningful trailing context (WHERE conditions, LIMIT, etc.).
func truncateMiddle(s string, max int) string {
	marker := " " + TruncationText + " "
	available := max - len(marker)
	if available <= 0 {
		return marker
	}

	// 2/3 of available space goes to the prefix, 1/3 to the suffix.
	prefixLen := (available * 2) / 3
	suffixLen := available - prefixLen

	return s[:prefixLen] + marker + s[len(s)-suffixLen:]
}

// TruncateForUI is used when displaying queries on various Vitess status pages
// to keep the pages small enough to load and render properly
func (p *Parser) TruncateForUI(query string) string {
	return TruncateQuery(query, p.truncateUILen)
}

// TruncateForLog is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func (p *Parser) TruncateForLog(query string) string {
	return TruncateQuery(query, p.truncateErrLen)
}
