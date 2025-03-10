package vtgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestVSchemaUpdate(t *testing.T) {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name:     sqlparser.NewIdentifierCI("uid"),
		Type:     querypb.Type_INT64,
		Nullable: true,
	}, {
		Name:     sqlparser.NewIdentifierCI("name"),
		Type:     querypb.Type_VARCHAR,
		Nullable: true,
	}}
	ks := &vindexes.Keyspace{Name: "ks"}
	tblNoCol := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, ColumnListAuthoritative: true}
	tblCol1 := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols1, ColumnListAuthoritative: true}
	tblCol2 := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2, ColumnListAuthoritative: true}
	tblCol2NA := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2}

	vindexTable_multicol_t1 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("multicol_t1"),
		Keyspace:                ks,
		Columns:                 cols2,
		ColumnListAuthoritative: true,
	}
	vindexTable_multicol_t2 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("multicol_t2"),
		Keyspace:                ks,
		Columns:                 cols2,
		ColumnListAuthoritative: true,
	}
	vindexTable_t1 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("t1"),
		Keyspace:                ks,
		Columns:                 cols1,
		ColumnListAuthoritative: true,
	}
	vindexTable_t2 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("t2"),
		Keyspace:                ks,
		Columns:                 cols1,
		ColumnListAuthoritative: true,
	}
	sqlparserCols1 := sqlparser.MakeColumns("id")
	sqlparserCols2 := sqlparser.MakeColumns("uid", "name")

	vindexTable_multicol_t1.ChildForeignKeys = append(vindexTable_multicol_t1.ChildForeignKeys, vindexes.ChildFKInfo{
		Table:         vindexTable_multicol_t2,
		ChildColumns:  sqlparserCols2,
		ParentColumns: sqlparserCols2,
		OnDelete:      sqlparser.NoAction,
		OnUpdate:      sqlparser.Restrict,
	})
	vindexTable_multicol_t2.ParentForeignKeys = append(vindexTable_multicol_t2.ParentForeignKeys, vindexes.ParentFKInfo{
		Table:         vindexTable_multicol_t1,
		ChildColumns:  sqlparserCols2,
		ParentColumns: sqlparserCols2,
	})
	vindexTable_t1.ChildForeignKeys = append(vindexTable_t1.ChildForeignKeys, vindexes.ChildFKInfo{
		Table:         vindexTable_t2,
		ChildColumns:  sqlparserCols1,
		ParentColumns: sqlparserCols1,
		OnDelete:      sqlparser.SetNull,
		OnUpdate:      sqlparser.Cascade,
	})
	vindexTable_t2.ParentForeignKeys = append(vindexTable_t2.ParentForeignKeys, vindexes.ParentFKInfo{
		Table:         vindexTable_t1,
		ChildColumns:  sqlparserCols1,
		ParentColumns: sqlparserCols1,
	})

	idxTbl1 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("idxTbl1"),
		Keyspace:                ks,
		ColumnListAuthoritative: true,
		PrimaryKey:              sqlparser.Columns{sqlparser.NewIdentifierCI("a")},
		UniqueKeys: [][]sqlparser.Expr{
			{sqlparser.NewColName("b")},
			{sqlparser.NewColName("c"), sqlparser.NewColName("d")},
		},
	}
	idxTbl2 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("idxTbl2"),
		Keyspace:                ks,
		ColumnListAuthoritative: true,
		PrimaryKey:              sqlparser.Columns{sqlparser.NewIdentifierCI("a")},
		UniqueKeys: [][]sqlparser.Expr{
			{&sqlparser.BinaryExpr{Operator: sqlparser.DivOp, Left: sqlparser.NewColName("b"), Right: sqlparser.NewIntLiteral("2")}},
			{sqlparser.NewColName("c"), &sqlparser.BinaryExpr{Operator: sqlparser.PlusOp, Left: sqlparser.NewColName("d"), Right: sqlparser.NewColName("e")}},
		},
	}

	tcases := []struct {
		name           string
		srvVschema     *vschemapb.SrvVSchema
		currentVSchema *vindexes.VSchema
		schema         map[string]*vindexes.TableInfo
		expected       *vindexes.VSchema
	}{{
		name: "0 Schematracking- 1 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol2NA}),
	}, {
		name:       "1 Schematracking- 0 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol1}),
	}, {
		name:       "1 Schematracking - 1 srvVSchema (no columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {}}),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (no columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {
			ColumnListAuthoritative: true,
		}}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblNoCol}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: true,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema tracker will be ignored for authoritative tables.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol2}),
	}, {
		name:     "srvVschema received as nil",
		schema:   map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected: makeTestEmptyVSchema(),
	}, {
		name:           "srvVschema received as nil - have existing vschema",
		currentVSchema: &vindexes.VSchema{},
		schema:         map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected:       &vindexes.VSchema{},
	}, {
		name:           "foreign keys in schema",
		currentVSchema: &vindexes.VSchema{},
		schema: map[string]*vindexes.TableInfo{
			"t1": {
				Columns: cols1,
			},
			"t2": {
				Columns: cols1,
				ForeignKeys: []*sqlparser.ForeignKeyDefinition{
					{
						Source: sqlparser.MakeColumns("id"),
						ReferenceDefinition: &sqlparser.ReferenceDefinition{
							ReferencedTable:   sqlparser.NewTableName("t1"),
							ReferencedColumns: sqlparserCols1,
							OnUpdate:          sqlparser.Cascade,
							OnDelete:          sqlparser.SetNull,
						},
					},
				},
			},
			"multicol_t1": {
				Columns: cols2,
			},
			"multicol_t2": {
				Columns: cols2,
				ForeignKeys: []*sqlparser.ForeignKeyDefinition{
					{
						Source: sqlparser.MakeColumns("uid", "name"),
						ReferenceDefinition: &sqlparser.ReferenceDefinition{
							ReferencedTable:   sqlparser.NewTableName("multicol_t1"),
							ReferencedColumns: sqlparserCols2,
							OnUpdate:          sqlparser.Restrict,
							OnDelete:          sqlparser.NoAction,
						},
					},
				},
			},
		},
		srvVschema: &vschemapb.SrvVSchema{
			Keyspaces: map[string]*vschemapb.Keyspace{
				"ks": {
					Sharded:        false,
					ForeignKeyMode: vschemapb.Keyspace_managed,
					Tables: map[string]*vschemapb.Table{
						"t1": {Columns: []*vschemapb.Column{{Name: "id", Type: querypb.Type_INT64}}},
						"t2": {Columns: []*vschemapb.Column{{Name: "id", Type: querypb.Type_INT64}}},
						"multicol_t1": {
							Columns: []*vschemapb.Column{
								{Name: "uid", Type: querypb.Type_INT64},
								{Name: "name", Type: querypb.Type_VARCHAR},
							},
						},
						"multicol_t2": {
							Columns: []*vschemapb.Column{
								{Name: "uid", Type: querypb.Type_INT64},
								{Name: "name", Type: querypb.Type_VARCHAR},
							},
						},
					},
				},
			},
		},
		expected: &vindexes.VSchema{
			MirrorRules:  map[string]*vindexes.MirrorRule{},
			RoutingRules: map[string]*vindexes.RoutingRule{},
			Keyspaces: map[string]*vindexes.KeyspaceSchema{
				"ks": {
					Keyspace:       ks,
					ForeignKeyMode: vschemapb.Keyspace_managed,
					Vindexes:       map[string]vindexes.Vindex{},
					Tables: map[string]*vindexes.BaseTable{
						"t1":          vindexTable_t1,
						"t2":          vindexTable_t2,
						"multicol_t1": vindexTable_multicol_t1,
						"multicol_t2": vindexTable_multicol_t2,
					},
				},
			},
		},
	}, {
		name:           "indexes in schema using columns",
		currentVSchema: &vindexes.VSchema{},
		schema: map[string]*vindexes.TableInfo{
			"idxTbl1": {
				Indexes: []*sqlparser.IndexDefinition{{
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypePrimary},
					Columns: []*sqlparser.IndexColumn{
						{Column: sqlparser.NewIdentifierCI("a")},
					},
				}, {
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypeUnique},
					Columns: []*sqlparser.IndexColumn{
						{Column: sqlparser.NewIdentifierCI("b")},
					},
				}, {
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypeDefault},
					Columns: []*sqlparser.IndexColumn{
						{Column: sqlparser.NewIdentifierCI("x")},
						{Column: sqlparser.NewIdentifierCI("y")},
					},
				}, {
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypeUnique},
					Columns: []*sqlparser.IndexColumn{
						{Column: sqlparser.NewIdentifierCI("c")},
						{Column: sqlparser.NewIdentifierCI("d")},
					},
				}},
			},
		},
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"idxTbl1": idxTbl1}),
	}, {
		name:           "indexes in schema using expressions",
		currentVSchema: &vindexes.VSchema{},
		schema: map[string]*vindexes.TableInfo{
			"idxTbl2": {
				Indexes: []*sqlparser.IndexDefinition{{
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypePrimary},
					Columns: []*sqlparser.IndexColumn{
						{Column: sqlparser.NewIdentifierCI("a")},
					},
				}, {
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypeUnique},
					Columns: []*sqlparser.IndexColumn{
						{Expression: &sqlparser.BinaryExpr{Operator: sqlparser.DivOp, Left: sqlparser.NewColName("b"), Right: sqlparser.NewIntLiteral("2")}},
					},
				}, {
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypeDefault},
					Columns: []*sqlparser.IndexColumn{
						{Expression: &sqlparser.BinaryExpr{Operator: sqlparser.PlusOp, Left: sqlparser.NewColName("x"), Right: sqlparser.NewColName("y")}},
					},
				}, {
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypeUnique},
					Columns: []*sqlparser.IndexColumn{
						{Column: sqlparser.NewIdentifierCI("c")},
						{Expression: &sqlparser.BinaryExpr{Operator: sqlparser.PlusOp, Left: sqlparser.NewColName("d"), Right: sqlparser.NewColName("e")}},
					},
				}},
			},
		},
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"idxTbl2": idxTbl2}),
	}}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs = nil
			vm.schema = &fakeSchema{t: tcase.schema}
			vm.currentSrvVschema = nil
			vm.currentVschema = tcase.currentVSchema
			vm.VSchemaUpdate(tcase.srvVschema, nil)

			utils.MustMatchFn(".globalTables", ".uniqueVindexes")(t, tcase.expected, vs)
			if tcase.srvVschema != nil {
				utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
			}
		})
	}
}

// TestKeyspaceRoutingRules tests that the vschema manager doens't panic in the presence of keyspace routing rules.
func TestKeyspaceRoutingRules(t *testing.T) {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	// Create a vschema manager with a fake vschema that returns a table with a column and a primary key.
	vm := &VSchemaManager{}
	vm.schema = &fakeSchema{t: map[string]*vindexes.TableInfo{
		"t1": {
			Columns: cols1,
			Indexes: []*sqlparser.IndexDefinition{
				{
					Info: &sqlparser.IndexInfo{Type: sqlparser.IndexTypePrimary},
					Columns: []*sqlparser.IndexColumn{
						{
							Column: sqlparser.NewIdentifierCI("id"),
						},
					},
				},
			},
		},
	}}
	// Define a vschema that has a keyspace routing rule.
	vs := &vindexes.VSchema{
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			"ks": {
				Tables:   map[string]*vindexes.BaseTable{},
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
			},
			"ks2": {
				Tables:   map[string]*vindexes.BaseTable{},
				Keyspace: &vindexes.Keyspace{Name: "ks2", Sharded: true},
			},
		},
		KeyspaceRoutingRules: map[string]string{
			"ks": "ks2",
		},
	}
	// Ensure that updating the vschema manager from the vschema doesn't cause a panic.
	vm.updateFromSchema(vs)
}

func TestRebuildVSchema(t *testing.T) {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name:     sqlparser.NewIdentifierCI("uid"),
		Type:     querypb.Type_INT64,
		Nullable: true,
	}, {
		Name:     sqlparser.NewIdentifierCI("name"),
		Type:     querypb.Type_VARCHAR,
		Nullable: true,
	}}
	ks := &vindexes.Keyspace{Name: "ks"}
	tblNoCol := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, ColumnListAuthoritative: true}
	tblCol1 := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols1, ColumnListAuthoritative: true}
	tblCol2 := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2, ColumnListAuthoritative: true}
	tblCol2NA := &vindexes.BaseTable{Name: sqlparser.NewIdentifierCS("tbl"), Keyspace: ks, Columns: cols2}

	tcases := []struct {
		name       string
		srvVschema *vschemapb.SrvVSchema
		schema     map[string]*vindexes.TableInfo
		expected   *vindexes.VSchema
	}{{
		name: "0 Schematracking- 1 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol2NA}),
	}, {
		name:       "1 Schematracking- 0 srvVSchema",
		srvVschema: makeTestSrvVSchema("ks", false, nil),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		expected:   makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol1}),
	}, {
		name:       "1 Schematracking - 1 srvVSchema (no columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {}}),
		schema:     map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) not authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: false,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol1}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (no columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{"tbl": {
			ColumnListAuthoritative: true,
		}}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema will override what srvSchema has.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblNoCol}),
	}, {
		name: "1 Schematracking - 1 srvVSchema (have columns) authoritative",
		srvVschema: makeTestSrvVSchema("ks", false, map[string]*vschemapb.Table{
			"tbl": {
				Columns:                 []*vschemapb.Column{{Name: "uid", Type: querypb.Type_INT64}, {Name: "name", Type: querypb.Type_VARCHAR}},
				ColumnListAuthoritative: true,
			},
		}),
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
		// schema tracker will be ignored for authoritative tables.
		expected: makeTestVSchema("ks", false, map[string]*vindexes.BaseTable{"tbl": tblCol2}),
	}, {
		name:   "srvVschema received as nil",
		schema: map[string]*vindexes.TableInfo{"tbl": {Columns: cols1}},
	}}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			vs = nil
			vm.schema = &fakeSchema{t: tcase.schema}
			vm.currentSrvVschema = tcase.srvVschema
			vm.currentVschema = nil
			vm.Rebuild()

			utils.MustMatchFn(".globalTables", ".uniqueVindexes")(t, tcase.expected, vs)
			if vs != nil {
				utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
			}
		})
	}
}

// TestVSchemaUDFsUpdate tests that the UDFs are updated in the VSchema.
func TestVSchemaUDFsUpdate(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "ks", Sharded: true}

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}
	vm.schema = &fakeSchema{udfs: []string{"udf1", "udf2"}}
	vm.VSchemaUpdate(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks": {Sharded: true},
		},
	}, nil)

	utils.MustMatchFn(".globalTables", ".uniqueVindexes")(t, &vindexes.VSchema{
		MirrorRules:  map[string]*vindexes.MirrorRule{},
		RoutingRules: map[string]*vindexes.RoutingRule{},
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			"ks": {
				Keyspace:       ks,
				ForeignKeyMode: vschemapb.Keyspace_unmanaged,
				Tables:         map[string]*vindexes.BaseTable{},
				Vindexes:       map[string]vindexes.Vindex{},
				AggregateUDFs:  []string{"udf1", "udf2"},
			},
		},
	}, vs)
	utils.MustMatch(t, vs, vm.currentVschema, "currentVschema does not match Vschema")
}

// TestVSchemaViewsUpdate tests that the views are updated in the VSchema.
func TestVSchemaViewsUpdate(t *testing.T) {
	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}

	s1 := &sqlparser.Select{
		From: sqlparser.TableExprs{sqlparser.NewAliasedTableExpr(sqlparser.NewTableName("t1"), "")},
	}
	s2 := &sqlparser.Select{
		From: sqlparser.TableExprs{sqlparser.NewAliasedTableExpr(sqlparser.NewTableName("t2"), "")},
	}
	s1.AddSelectExpr(sqlparser.NewAliasedExpr(sqlparser.NewIntLiteral("1"), ""))
	s2.AddSelectExpr(sqlparser.NewAliasedExpr(sqlparser.NewIntLiteral("2"), ""))
	vm.schema = &fakeSchema{v: map[string]sqlparser.TableStatement{
		"v1": s1,
		"v2": s2,
	}}

	vm.VSchemaUpdate(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks": {Sharded: true},
		},
	}, nil)

	// find in views map
	assert.NotNil(t, vs.FindView("ks", "v1"))
	assert.NotNil(t, vs.FindView("ks", "v2"))
	// find in global table
	assert.NotNil(t, vs.FindView("", "v1"))
	assert.NotNil(t, vs.FindView("", "v2"))

	utils.MustMatch(t, vs, vm.currentVschema, "currentVschema does not match Vschema")
}

func TestMarkErrorIfCyclesInFk(t *testing.T) {
	ksName := "ks"
	keyspace := &vindexes.Keyspace{
		Name: ksName,
	}
	tests := []struct {
		name       string
		getVschema func() *vindexes.VSchema
		errWanted  string
	}{
		{
			name: "Has a direct cycle",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
								"t3": {
									Name:     sqlparser.NewIdentifierCS("t3"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"col"}, "t1", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t3", createFkDefinition([]string{"col"}, "t2", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"col"}, "t3", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys",
		},
		{
			name: "Has a direct cycle but there is a restrict constraint in between",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
								"t3": {
									Name:     sqlparser.NewIdentifierCS("t3"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"col"}, "t1", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t3", createFkDefinition([]string{"col"}, "t2", []string{"col"}, sqlparser.Restrict, sqlparser.Restrict))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"col"}, "t3", []string{"col"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted: "",
		},
		{
			name: "No cycle",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
								"t3": {
									Name:     sqlparser.NewIdentifierCS("t3"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"col"}, "t1", []string{"col"}, sqlparser.Cascade, sqlparser.Cascade))
				_ = vschema.AddForeignKey("ks", "t3", createFkDefinition([]string{"col"}, "t2", []string{"col"}, sqlparser.Cascade, sqlparser.Cascade))
				return vschema
			},
			errWanted: "",
		}, {
			name: "Self-referencing foreign key with delete cascade",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("id"),
										},
										{
											Name: sqlparser.NewIdentifierCI("manager_id"),
										},
									},
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"manager_id"}, "t1", []string{"id"}, sqlparser.SetNull, sqlparser.Cascade))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys. Cycle exists between [ks.t1.id ks.t1.id]",
		}, {
			name: "Self-referencing foreign key without delete cascade",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("id"),
										},
										{
											Name: sqlparser.NewIdentifierCI("manager_id"),
										},
									},
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"manager_id"}, "t1", []string{"id"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted: "",
		}, {
			name: "Has an indirect cycle because of cascades",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("a"),
										},
										{
											Name: sqlparser.NewIdentifierCI("b"),
										},
										{
											Name: sqlparser.NewIdentifierCI("c"),
										},
									},
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
									Columns: []vindexes.Column{
										{
											Name: sqlparser.NewIdentifierCI("d"),
										},
										{
											Name: sqlparser.NewIdentifierCI("e"),
										},
										{
											Name: sqlparser.NewIdentifierCI("f"),
										},
									},
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"f"}, "t1", []string{"a"}, sqlparser.SetNull, sqlparser.Cascade))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"b"}, "t2", []string{"e"}, sqlparser.SetNull, sqlparser.Cascade))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys",
		}, {
			name: "Cycle part of a multi-column foreign key",
			getVschema: func() *vindexes.VSchema {
				vschema := &vindexes.VSchema{
					Keyspaces: map[string]*vindexes.KeyspaceSchema{
						ksName: {
							ForeignKeyMode: vschemapb.Keyspace_managed,
							Tables: map[string]*vindexes.BaseTable{
								"t1": {
									Name:     sqlparser.NewIdentifierCS("t1"),
									Keyspace: keyspace,
								},
								"t2": {
									Name:     sqlparser.NewIdentifierCS("t2"),
									Keyspace: keyspace,
								},
							},
						},
					},
				}
				_ = vschema.AddForeignKey("ks", "t2", createFkDefinition([]string{"e", "f"}, "t1", []string{"a", "b"}, sqlparser.SetNull, sqlparser.SetNull))
				_ = vschema.AddForeignKey("ks", "t1", createFkDefinition([]string{"b"}, "t2", []string{"e"}, sqlparser.SetNull, sqlparser.SetNull))
				return vschema
			},
			errWanted: "VT09019: keyspace 'ks' has cyclic foreign keys",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vschema := tt.getVschema()
			markErrorIfCyclesInFk(vschema)
			if tt.errWanted != "" {
				require.ErrorContains(t, vschema.Keyspaces[ksName].Error, tt.errWanted)
				return
			}
			require.NoError(t, vschema.Keyspaces[ksName].Error)
		})
	}
}

// TestVSchemaUpdateWithFKReferenceToInternalTables tests that any internal table as part of fk reference is ignored.
func TestVSchemaUpdateWithFKReferenceToInternalTables(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "ks"}
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	sqlparserCols1 := sqlparser.MakeColumns("id")

	vindexTable_t1 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("t1"),
		Keyspace:                ks,
		Columns:                 cols1,
		ColumnListAuthoritative: true,
	}
	vindexTable_t2 := &vindexes.BaseTable{
		Name:                    sqlparser.NewIdentifierCS("t2"),
		Keyspace:                ks,
		Columns:                 cols1,
		ColumnListAuthoritative: true,
	}

	vindexTable_t1.ChildForeignKeys = append(vindexTable_t1.ChildForeignKeys, vindexes.ChildFKInfo{
		Table:         vindexTable_t2,
		ChildColumns:  sqlparserCols1,
		ParentColumns: sqlparserCols1,
		OnDelete:      sqlparser.SetNull,
		OnUpdate:      sqlparser.Cascade,
	})
	vindexTable_t2.ParentForeignKeys = append(vindexTable_t2.ParentForeignKeys, vindexes.ParentFKInfo{
		Table:         vindexTable_t1,
		ChildColumns:  sqlparserCols1,
		ParentColumns: sqlparserCols1,
	})

	vm := &VSchemaManager{}
	var vs *vindexes.VSchema
	vm.subscriber = func(vschema *vindexes.VSchema, _ *VSchemaStats) {
		vs = vschema
		vs.ResetCreated()
	}
	vm.schema = &fakeSchema{t: map[string]*vindexes.TableInfo{
		"t1": {Columns: cols1},
		"t2": {
			Columns: cols1,
			ForeignKeys: []*sqlparser.ForeignKeyDefinition{
				createFkDefinition([]string{"id"}, "t1", []string{"id"}, sqlparser.Cascade, sqlparser.SetNull),
				createFkDefinition([]string{"id"}, "_vt_HOLD_6ace8bcef73211ea87e9f875a4d24e90_20200915120410", []string{"id"}, sqlparser.Cascade, sqlparser.SetNull),
			},
		},
	}}
	vm.VSchemaUpdate(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"ks": {
				ForeignKeyMode: vschemapb.Keyspace_managed,
				Tables: map[string]*vschemapb.Table{
					"t1": {Columns: []*vschemapb.Column{{Name: "id", Type: querypb.Type_INT64}}},
					"t2": {Columns: []*vschemapb.Column{{Name: "id", Type: querypb.Type_INT64}}},
				},
			},
		},
	}, nil)

	utils.MustMatchFn(".globalTables", ".uniqueVindexes")(t, &vindexes.VSchema{
		MirrorRules:  map[string]*vindexes.MirrorRule{},
		RoutingRules: map[string]*vindexes.RoutingRule{},
		Keyspaces: map[string]*vindexes.KeyspaceSchema{
			"ks": {
				Keyspace:       ks,
				ForeignKeyMode: vschemapb.Keyspace_managed,
				Vindexes:       map[string]vindexes.Vindex{},
				Tables: map[string]*vindexes.BaseTable{
					"t1": vindexTable_t1,
					"t2": vindexTable_t2,
				},
			},
		},
	}, vs)
	utils.MustMatch(t, vs, vm.currentVschema, "currentVschema should have same reference as Vschema")
}

// createFkDefinition is a helper function to create a Foreign key definition struct from the columns used in it provided as list of strings.
func createFkDefinition(childCols []string, parentTableName string, parentCols []string, onUpdate, onDelete sqlparser.ReferenceAction) *sqlparser.ForeignKeyDefinition {
	pKs, pTbl, _ := sqlparser.NewTestParser().ParseTable(parentTableName)
	return &sqlparser.ForeignKeyDefinition{
		Source: sqlparser.MakeColumns(childCols...),
		ReferenceDefinition: &sqlparser.ReferenceDefinition{
			ReferencedTable:   sqlparser.NewTableNameWithQualifier(pTbl, pKs),
			ReferencedColumns: sqlparser.MakeColumns(parentCols...),
			OnUpdate:          onUpdate,
			OnDelete:          onDelete,
		},
	}
}

func makeTestVSchema(ks string, sharded bool, tbls map[string]*vindexes.BaseTable) *vindexes.VSchema {
	keyspaceSchema := &vindexes.KeyspaceSchema{
		Keyspace: &vindexes.Keyspace{
			Name:    ks,
			Sharded: sharded,
		},
		// Default foreign key mode
		ForeignKeyMode: vschemapb.Keyspace_unmanaged,
		Tables:         tbls,
		Vindexes:       map[string]vindexes.Vindex{},
	}
	vs := makeTestEmptyVSchema()
	vs.Keyspaces[ks] = keyspaceSchema
	vs.ResetCreated()
	return vs
}

func makeTestEmptyVSchema() *vindexes.VSchema {
	return &vindexes.VSchema{
		MirrorRules:  map[string]*vindexes.MirrorRule{},
		RoutingRules: map[string]*vindexes.RoutingRule{},
		Keyspaces:    map[string]*vindexes.KeyspaceSchema{},
	}
}

func makeTestSrvVSchema(ks string, sharded bool, tbls map[string]*vschemapb.Table) *vschemapb.SrvVSchema {
	keyspaceSchema := &vschemapb.Keyspace{
		Sharded: sharded,
		Tables:  tbls,
		// Default foreign key mode
		ForeignKeyMode: vschemapb.Keyspace_unmanaged,
	}
	return &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{ks: keyspaceSchema},
	}
}

type fakeSchema struct {
	t    map[string]*vindexes.TableInfo
	v    map[string]sqlparser.TableStatement
	udfs []string
}

func (f *fakeSchema) Tables(string) map[string]*vindexes.TableInfo {
	return f.t
}

func (f *fakeSchema) Views(string) map[string]sqlparser.TableStatement {
	return f.v
}
func (f *fakeSchema) UDFs(string) []string { return f.udfs }

var _ SchemaInfo = (*fakeSchema)(nil)
