package sqlparser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateQuery(t *testing.T) {
	tests := []struct {
		query string
		max   int
		want  string
	}{
		{
			// max < marker length, so min is enforced (13 bytes)
			query: "select 111",
			max:   2,
			want:  "select 111", // not truncated because len(sql) <= enforced min
		},
		{
			// "select 1111" is 11 bytes; enforced min is 13.
			// 11 < 13 so len(sql) <= max after enforcement; no truncation.
			query: "select 1111",
			max:   2,
			want:  "select 1111",
		},
		{
			// "select 11111" is 12 bytes; enforced min is 13.
			// 12 < 13 so no truncation.
			query: "select 11111",
			max:   2,
			want:  "select 11111",
		},
		{
			// "select * from test where name = 'abc'" is 38 bytes, max=30.
			// marker " [TRUNCATED] " is 13 bytes, available = 30-13 = 17.
			// prefixLen = (17*2)/3 = 11, suffixLen = 17-11 = 6.
			// prefix: "select * fr", suffix: " 'abc'"
			query: "select * from test where name = 'abc'",
			max:   30,
			want:  "select * fr [TRUNCATED]  'abc'",
		},
		{
			query: "select * from test where name = 'abc'",
			max:   1005,
			want:  "select * from test where name = 'abc'",
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s-%d", tt.query, tt.max), func(t *testing.T) {
			got := TruncateQuery(tt.query, tt.max)
			assert.Equalf(t, tt.want, got, "TruncateQuery(%v, %v)", tt.query, tt.max)
		})
	}
}

func TestTruncateQuery_PreservesTrailingComment(t *testing.T) {
	// Simulate a real webapp query with InfoQueryComment trailing comment
	sql := "select `id`, `name` from `users` where `team_id` = 123 and `id` in (1,2,3,4,5,6,7,8,9,10)"
	comment := " /* script(req123): trace|TEAM:123|HOST:host|VTQUERYLOG_SAMPLE_PROB:0.0100000000 */"
	query := sql + comment

	// Truncate to a length shorter than the SQL body but longer than the marker
	result := TruncateQuery(query, 60)

	// The trailing comment must always be preserved
	assert.True(t, strings.HasSuffix(result, comment),
		"trailing comment must be preserved, got: %s", result)
	assert.Contains(t, result, TruncationText)
}

func TestTruncateQuery_MiddleTruncation(t *testing.T) {
	// A query with a large IN-clause in the middle and important WHERE at the end
	prefix := "select * from users where team_id = 123 and id in ("
	inClause := strings.Repeat("12345,", 100) // large repetitive middle section
	suffix := ") and status = 'active' order by created_at limit 10"
	query := prefix + inClause + suffix

	result := TruncateQuery(query, 120)

	// The result must contain the truncation marker
	assert.Contains(t, result, TruncationText)

	// The result preserves the beginning of the query
	assert.True(t, strings.HasPrefix(result, "select * from users"),
		"beginning of query should be preserved, got: %s", result)

	// The result preserves the end of the query
	assert.True(t, strings.HasSuffix(result, "limit 10"),
		"end of query should be preserved, got: %s", result)

	// The result respects the max length
	assert.LessOrEqual(t, len(result), 120,
		"result length %d exceeds max 120", len(result))
}

func TestTruncateQuery_NoTruncationNeeded(t *testing.T) {
	query := "select 1"
	assert.Equal(t, "select 1", TruncateQuery(query, 100))
	assert.Equal(t, "select 1", TruncateQuery(query, 0)) // 0 means no truncation
}

func TestTruncateQuery_WithLeadingAndTrailingComments(t *testing.T) {
	query := "/* leading */ select * from a_long_table_name where id = 1 /* trailing */"

	result := TruncateQuery(query, 30)

	// Both margin comments should be preserved
	assert.True(t, strings.HasPrefix(result, "/* leading */"),
		"leading comment must be preserved, got: %s", result)
	assert.True(t, strings.HasSuffix(result, "/* trailing */"),
		"trailing comment must be preserved, got: %s", result)
	assert.Contains(t, result, TruncationText)
}

func TestTruncateMiddle(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		max    int
		expect string
	}{
		{
			// max=20, marker=13, available=7, prefix=(7*2)/3=4, suffix=3
			name:   "basic middle truncation",
			input:  "abcdefghijklmnopqrstuvwxyz",
			max:    20,
			expect: "abcd [TRUNCATED] xyz",
		},
		{
			// max < marker length: available<=0, returns just the marker
			name:   "max smaller than marker returns marker",
			input:  "abcdefghijklmnopqrstuvwxyz",
			max:    5,
			expect: " [TRUNCATED] ",
		},
		{
			// max == marker length: available=0, returns just the marker
			name:   "exactly marker size",
			input:  "abcdefghijklmnopqrstuvwxyz",
			max:    13, // len(" [TRUNCATED] ") == 13
			expect: " [TRUNCATED] ",
		},
		{
			// max=14, available=1, prefix=(1*2)/3=0, suffix=1-0=1
			name:   "one byte over marker goes to suffix",
			input:  "abcdefghijklmnopqrstuvwxyz",
			max:    14,
			expect: " [TRUNCATED] z",
		},
		{
			// max=15, available=2, prefix=(2*2)/3=1, suffix=2-1=1
			name:   "two bytes available split 1-1",
			input:  "abcdefghijklmnopqrstuvwxyz",
			max:    15,
			expect: "a [TRUNCATED] z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateMiddle(tt.input, tt.max)
			assert.Equal(t, tt.expect, got)
			// When max >= marker length, result should fit within max.
			// When max < marker length, we always return the marker (which is the minimum output).
			if tt.max >= len(" [TRUNCATED] ") {
				assert.LessOrEqual(t, len(got), tt.max,
					"result length %d exceeds max %d", len(got), tt.max)
			}
		})
	}
}
