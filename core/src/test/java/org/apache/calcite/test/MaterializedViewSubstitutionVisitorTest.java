/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for SubstutionVisitor.
 */
public class MaterializedViewSubstitutionVisitorTest extends AbstractMaterializedViewTest {

  @Test public void testFilter() {
    checkMaterialize(
        "select * from \"emps\" where \"deptno\" = 10",
        "select \"empid\" + 1 from \"emps\" where \"deptno\" = 10");
  }

  @Test public void testFilterToProject0() {
    checkMaterialize(
        "select *, \"empid\" * 2 from \"emps\"",
        "select * from \"emps\" where (\"empid\" * 2) > 3");
  }

  @Test public void testFilterQueryOnProjectView() {
    checkMaterialize(
        "select \"deptno\", \"empid\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10");
  }

  @Test public void testFilterToProject1() {
    checkNoMaterialize(
        "select \"deptno\", \"salary\" from \"emps\"",
        "select \"empid\", \"deptno\", \"salary\"\n"
            + "from \"emps\" where (\"salary\" * 0.8) > 10000");
  }

  /** Runs the same test as {@link #testFilterQueryOnProjectView()} but more
   * concisely. */
  @Test public void testFilterQueryOnProjectView0() {
    checkMaterialize(
        "select \"deptno\", \"empid\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnProjectView()} but with extra column in
   * materialized view. */
  @Test public void testFilterQueryOnProjectView1() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnProjectView()} but with extra column in both
   * materialized view and query. */
  @Test public void testFilterQueryOnProjectView2() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\"",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10");
  }

  @Test public void testFilterQueryOnProjectView3() {
    checkMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" - 10 = 0");
  }

  /** As {@link #testFilterQueryOnProjectView3()} but materialized view cannot
   * be used because it does not contain required expression. */
  @Test public void testFilterQueryOnProjectView4() {
    checkNoMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" + 10 = 20");
  }

  /** As {@link #testFilterQueryOnProjectView3()} but also contains an
   * expression column. */
  @Test public void testFilterQueryOnProjectView5() {
    checkMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\" + 1 as ee, \"name\" from \"emps\"",
        "select \"name\", \"empid\" + 1 as e from \"emps\" where \"deptno\" - 10 = 2",
        resultContains(""
            + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[2], "
            + "expr#4=[=($t0, $t3)], name=[$t2], E=[$t1], $condition=[$t4])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]]"));
  }

  /** Cannot materialize because "name" is not projected in the MV. */
  @Test public void testFilterQueryOnProjectView6() {
    checkNoMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\"  from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" - 10 = 0");
  }

  /** As {@link #testFilterQueryOnProjectView3()} but also contains an
   * expression column. */
  @Test public void testFilterQueryOnProjectView7() {
    checkNoMaterialize(
        "select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\", \"empid\" + 2 from \"emps\" where \"deptno\" - 10 = 0");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-988">[CALCITE-988]
   * FilterToProjectUnifyRule.invert(MutableRel, MutableRel, MutableProject)
   * works incorrectly</a>. */
  @Test public void testFilterQueryOnProjectView8() {
    String mv = ""
        + "select \"salary\", \"commission\",\n"
        + "\"deptno\", \"empid\", \"name\" from \"emps\"";
    String query = ""
        + "select *\n"
        + "from (select * from \"emps\" where \"name\" is null)\n"
        + "where \"commission\" is null";
    checkMaterialize(mv, query);
  }

  @Test public void testFilterQueryOnFilterView() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\" where \"deptno\" = 10",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. */
  @Test public void testFilterQueryOnFilterView2() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\" where \"deptno\" = 10",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" "
            + "where \"deptno\" = 10 and \"empid\" < 150");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * view. */
  @Test public void testFilterQueryOnFilterView3() {
    checkMaterialize(
        "select \"deptno\", \"empid\", \"name\" from \"emps\"\n"
            + "where \"deptno\" = 10 or \"deptno\" = 20 or \"empid\" < 160",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10",
        resultContains(""
            + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[+($t1, $t3)], expr#5=[10], "
            + "expr#6=[CAST($t0):INTEGER NOT NULL], expr#7=[=($t5, $t6)], X=[$t4], "
            + "name=[$t2], $condition=[$t7])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. */
  @Test public void testFilterQueryOnFilterView4() {
    checkMaterialize(
        "select * from \"emps\" where \"deptno\" > 10",
        "select \"name\" from \"emps\" where \"deptno\" > 30");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query and columns selected are subset of columns in materialized view. */
  @Test public void testFilterQueryOnFilterView5() {
    checkMaterialize(
        "select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10",
        "select \"name\" from \"emps\" where \"deptno\" > 30");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query and columns selected are subset of columns in materialized view. */
  @Test public void testFilterQueryOnFilterView6() {
    checkMaterialize(
        "select \"name\", \"deptno\", \"salary\" from \"emps\" "
          + "where \"salary\" > 2000.5",
        "select \"name\" from \"emps\" where \"deptno\" > 30 and \"salary\" > 3000");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  @Test public void testFilterQueryOnFilterView7() {
    checkMaterialize(
        "select * from \"emps\" where "
            + "((\"salary\" < 1111.9 and \"deptno\" > 10)"
            + "or (\"empid\" > 400 and \"salary\" > 5000) "
            + "or \"salary\" > 500)",
        "select \"name\" from \"emps\" where (\"salary\" > 1000 "
            + "or (\"deptno\" >= 30 and \"salary\" <= 500))");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. However, columns selected are not present in columns of materialized
   * view, Hence should not use materialized view. */
  @Test public void testFilterQueryOnFilterView8() {
    checkNoMaterialize(
        "select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10",
        "select \"name\", \"empid\" from \"emps\" where \"deptno\" > 30");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * query. */
  @Test public void testFilterQueryOnFilterView9() {
    checkNoMaterialize(
        "select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10",
        "select \"name\", \"empid\" from \"emps\" "
            + "where \"deptno\" > 30 or \"empid\" > 10");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition currently
   * has unsupported type being checked on query. */
  @Test public void testFilterQueryOnFilterView10() {
    checkNoMaterialize(
        "select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10 "
            + "and \"name\" = \'calcite\'",
        "select \"name\", \"empid\" from \"emps\" where \"deptno\" > 30 "
            + "or \"empid\" > 10");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  @Test public void testFilterQueryOnFilterView11() {
    checkNoMaterialize(
        "select \"name\", \"deptno\" from \"emps\" where "
            + "(\"salary\" < 1111.9 and \"deptno\" > 10)"
            + "or (\"empid\" > 400 and \"salary\" > 5000)",
        "select \"name\" from \"emps\" where \"deptno\" > 30 and \"salary\" > 3000");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition of
   * query is stronger but is on the column not present in MV (salary).
   */
  @Test public void testFilterQueryOnFilterView12() {
    checkNoMaterialize(
        "select \"name\", \"deptno\" from \"emps\" where \"salary\" > 2000.5",
        "select \"name\" from \"emps\" where \"deptno\" > 30 and \"salary\" > 3000");
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  @Test public void testFilterQueryOnFilterView13() {
    checkNoMaterialize(
        "select * from \"emps\" where "
            + "(\"salary\" < 1111.9 and \"deptno\" > 10)"
            + "or (\"empid\" > 400 and \"salary\" > 5000)",
        "select \"name\" from \"emps\" where \"salary\" > 1000 "
            + "or (\"deptno\" > 30 and \"salary\" > 3000)");
  }

  /** As {@link #testFilterQueryOnFilterView7()} but columns in materialized
   * view are a permutation of columns in the query. */
  @Test public void testFilterQueryOnFilterView14() {
    String q = "select * from \"emps\" where (\"salary\" > 1000 "
        + "or (\"deptno\" >= 30 and \"salary\" <= 500))";
    String m = "select \"deptno\", \"empid\", \"name\", \"salary\", \"commission\" "
        + "from \"emps\" as em where "
        + "((\"salary\" < 1111.9 and \"deptno\" > 10)"
        + "or (\"empid\" > 400 and \"salary\" > 5000) "
        + "or \"salary\" > 500)";
    checkMaterialize(m, q);
  }

  /** As {@link #testFilterQueryOnFilterView13()} but using alias
   * and condition of query is stronger. */
  @Test public void testAlias() {
    checkMaterialize(
        "select * from \"emps\" as em where "
            + "(em.\"salary\" < 1111.9 and em.\"deptno\" > 10)"
            + "or (em.\"empid\" > 400 and em.\"salary\" > 5000)",
        "select \"name\" as n from \"emps\" as e where "
            + "(e.\"empid\" > 500 and e.\"salary\" > 6000)");
  }

  /** Aggregation query at same level of aggregation as aggregation
   * materialization. */
  @Test public void testAggregate0() {
    checkMaterialize(
        "select count(*) as c from \"emps\" group by \"empid\"",
        "select count(*) + 1 as c from \"emps\" group by \"empid\"");
  }

  /**
   * Aggregation query at same level of aggregation as aggregation
   * materialization but with different row types. */
  @Test public void testAggregate1() {
    checkMaterialize(
        "select count(*) as c0 from \"emps\" group by \"empid\"",
        "select count(*) as c1 from \"emps\" group by \"empid\"");
  }

  @Test public void testAggregate2() {
    checkMaterialize(
        "select \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"deptno\"",
        "select count(*) + 1 as c, \"deptno\" from \"emps\" group by \"deptno\"");
  }

  @Test public void testAggregate3() {
    String mv = ""
        + "select \"deptno\", sum(\"salary\"), sum(\"commission\"), sum(\"k\")\n"
        + "from\n"
        + "  (select \"deptno\", \"salary\", \"commission\", 100 as \"k\"\n"
        + "  from \"emps\")\n"
        + "group by \"deptno\"";
    String query = ""
        + "select \"deptno\", sum(\"salary\"), sum(\"k\")\n"
        + "from\n"
        + "  (select \"deptno\", \"salary\", 100 as \"k\"\n"
        + "  from \"emps\")\n"
        + "group by \"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testAggregate4() {
    String mv = ""
        + "select \"deptno\", \"commission\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\", \"commission\"";
    String query = ""
        + "select \"deptno\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "where \"commission\" = 100\n"
        + "group by \"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testAggregate5() {
    String mv = ""
        + "select \"deptno\" + \"commission\", \"commission\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\" + \"commission\", \"commission\"";
    String query = ""
        + "select \"commission\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "where \"commission\" * (\"deptno\" + \"commission\") = 100\n"
        + "group by \"commission\"";
    checkMaterialize(mv, query);
  }

  /**
   * Matching failed because the filtering condition under Aggregate
   * references columns for aggregation.
   */
  @Test public void testAggregate6() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "where \"salary\" > 1000\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    checkNoMaterialize(mv, query);
  }

  /**
   * There will be a compensating Project added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  @Test public void testCompensatingCalcWithAggregate0() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    checkMaterialize(mv, query);
  }

  /**
   * There will be a compensating Project + Filter added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  @Test public void testCompensatingCalcWithAggregate1() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" >=20\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    checkMaterialize(mv, query);
  }

  /**
   * There will be a compensating Project + Filter added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  @Test public void testCompensatingCalcWithAggregate2() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "where \"deptno\" >= 10\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" >= 20\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 20";
    checkMaterialize(mv, query);
  }

  /** Aggregation query at same level of aggregation as aggregation
   * materialization with grouping sets. */
  @Test public void testAggregateGroupSets1() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"salary\") as s\n"
            + "from \"emps\" group by cube(\"empid\",\"deptno\")",
        "select count(*) + 1 as c, \"deptno\"\n"
            + "from \"emps\" group by cube(\"empid\",\"deptno\")");
  }

  /** Aggregation query with different grouping sets, should not
   * do materialization. */
  @Test public void testAggregateGroupSets2() {
    checkNoMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"salary\") as s\n"
            + "from \"emps\" group by cube(\"empid\",\"deptno\")",
        "select count(*) + 1 as c, \"deptno\"\n"
            + "from \"emps\" group by rollup(\"empid\",\"deptno\")");
  }

  /** Aggregation query at coarser level of aggregation than aggregation
   * materialization. Requires an additional aggregate to roll up. Note that
   * COUNT is rolled up using SUM0. */
  @Test public void testAggregateRollUp() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c, \"deptno\" from \"emps\" group by \"deptno\"",
        resultContains(""
            + "LogicalCalc(expr#0..1=[{inputs}], expr#2=[1], "
            + "expr#3=[+($t1, $t2)], C=[$t3], deptno=[$t0])\n"
            + "  LogicalAggregate(group=[{1}], agg#0=[$SUM0($2)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  /** Aggregation query with groupSets at coarser level of aggregation than
   * aggregation materialization. Requires an additional aggregate to roll up.
   * Note that COUNT is rolled up using SUM0. */
  @Test public void testAggregateGroupSetsRollUp() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"salary\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c, \"deptno\"\n"
            + "from \"emps\" group by cube(\"empid\",\"deptno\")",
        resultContains(""
            + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], "
            + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
            + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[$SUM0($2)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateGroupSetsRollUp2() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s from \"emps\" "
            + "group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c,  \"deptno\" from \"emps\" group by cube(\"empid\",\"deptno\")",
        resultContains(""
            + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], "
            + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
            + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[$SUM0($2)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3087">[CALCITE-3087]
   * AggregateOnProjectToAggregateUnifyRule ignores Project incorrectly when its
   * Mapping breaks ordering</a>. */
  @Test public void testAggregateOnProject1() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s from \"emps\" "
            + "group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c, \"deptno\" from \"emps\" group by \"deptno\", \"empid\"");
  }

  @Test public void testAggregateOnProject2() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"salary\") as s from \"emps\" "
            + "group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c,  \"deptno\" from \"emps\" group by cube(\"deptno\", \"empid\")",
        resultContains(""
            + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], "
            + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
            + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[$SUM0($2)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateOnProject3() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"salary\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c,  \"deptno\"\n"
            + "from \"emps\" group by rollup(\"deptno\", \"empid\")",
        resultContains(""
            + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], "
            + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
            + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {1}, {}]], agg#0=[$SUM0($2)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateOnProject4() {
    checkMaterialize(
        "select \"salary\", \"empid\", \"deptno\", count(*) as c, sum(\"commission\") as s\n"
            + "from \"emps\" group by \"salary\", \"empid\", \"deptno\"",
        "select count(*) + 1 as c,  \"deptno\"\n"
            + "from \"emps\" group by rollup(\"empid\", \"deptno\", \"salary\")",
        resultContains(""
            + "LogicalCalc(expr#0..3=[{inputs}], expr#4=[1], "
            + "expr#5=[+($t3, $t4)], C=[$t5], deptno=[$t2])\n"
            + "  LogicalAggregate(group=[{0, 1, 2}], groups=[[{0, 1, 2}, {1, 2}, {1}, {}]], agg#0=[$SUM0($3)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateOnProjectAndFilter() {
    String mv = ""
        + "select \"deptno\", sum(\"salary\"), count(1)\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    String query = ""
        + "select \"deptno\", count(1)\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10\n"
        + "group by \"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testProjectOnProject() {
    String mv = ""
        + "select \"deptno\", sum(\"salary\") + 2, sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    String query = ""
        + "select \"deptno\", sum(\"salary\") + 2\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testPermutationError() {
    checkMaterialize(
        "select min(\"salary\"), count(*), max(\"salary\"), sum(\"salary\"), \"empid\" "
            + "from \"emps\" group by \"empid\"",
        "select count(*), \"empid\" from \"emps\" group by \"empid\"");
  }

  @Test public void testJoinOnLeftProjectToJoin() {
    String mv = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testJoinOnRightProjectToJoin() {
    String mv = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\"\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testJoinOnProjectsToJoin() {
    String mv = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\"\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testJoinOnCalcToJoin0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"A\".\"empid\", \"A\".\"deptno\", \"depts\".\"deptno\" from\n"
        + " (select \"empid\", \"deptno\" from \"emps\" where \"deptno\" > 10) A"
        + " join \"depts\"\n"
        + "on \"A\".\"deptno\" = \"depts\".\"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testJoinOnCalcToJoin1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testJoinOnCalcToJoin2() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    checkMaterialize(mv, query);
  }

  @Test public void testJoinOnCalcToJoin3() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" + 1 as \"deptno\" from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    // Match failure because join condition references non-mapping projects.
    checkNoMaterialize(mv, query);
  }

  @Test public void testJoinOnCalcToJoin4() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" from \"emps\" where \"empid\" is not null) A\n"
        + "full join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" is not null) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    // Match failure because of outer join type but filtering condition in Calc is not empty.
    checkNoMaterialize(mv, query);
  }

  @Test public void testJoinMaterialization() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join \"depts\" using (\"deptno\")";
    checkMaterialize("select * from \"emps\" where \"empid\" < 500", q);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-891">[CALCITE-891]
   * TableScan without Project cannot be substituted by any projected
   * materialization</a>. */
  @Test public void testJoinMaterialization2() {
    String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    String m = "select \"deptno\", \"empid\", \"name\",\n"
        + "\"salary\", \"commission\" from \"emps\"";
    checkMaterialize(m, q);
  }

  @Test public void testJoinMaterialization3() {
    String q = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" = 1";
    String m = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    checkMaterialize(m, q);
  }

  @Test public void testUnionAll() {
    String q = "select * from \"emps\" where \"empid\" > 300\n"
        + "union all select * from \"emps\" where \"empid\" < 200";
    String m = "select * from \"emps\" where \"empid\" < 500";
    checkMaterialize(m, q,
        resultContains(""
            + "LogicalUnion(all=[true])\n"
            + "  LogicalCalc(expr#0..4=[{inputs}], expr#5=[300], expr#6=[>($t0, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "    EnumerableTableScan(table=[[hr, emps]])\n"
            + "  LogicalCalc(expr#0..4=[{inputs}], expr#5=[200], expr#6=[<($t0, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testTableModify() {
    String m = "select \"deptno\", \"empid\", \"name\""
        + "from \"emps\" where \"deptno\" = 10";
    String q = "upsert into \"dependents\""
        + "select \"empid\" + 1 as x, \"name\""
        + "from \"emps\" where \"deptno\" = 10";
    checkMaterialize(m, q);
  }

  @Test public void testSingleMaterializationMultiUsage() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";
    String m = "select * from \"emps\" where \"empid\" < 500";
    checkMaterialize(m, q,
        resultContains(""
            + "LogicalCalc(expr#0..9=[{inputs}], proj#0..4=[{exprs}], deptno0=[$t6], name0=[$t7], salary0=[$t8], commission0=[$t9])\n"
            + "  LogicalJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "    LogicalCalc(expr#0..4=[{inputs}], expr#5=[300], expr#6=[<($t0, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    LogicalCalc(expr#0..4=[{inputs}], expr#5=[200], expr#6=[<($t0, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testMaterializationOnJoinQuery() {
    checkMaterialize(
        "select * from \"emps\" where \"empid\" < 500",
        "select *\n"
            + "from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" < 300 ");
  }

  @Test public void testMaterializationAfterTrimingOfUnusedFields() {
    String sql =
        "select \"y\".\"deptno\", \"y\".\"name\", \"x\".\"sum_salary\"\n"
            + "from\n"
            + "  (select \"deptno\", sum(\"salary\") \"sum_salary\"\n"
            + "  from \"emps\"\n"
            + "  group by \"deptno\") \"x\"\n"
            + "  join\n"
            + "  \"depts\" \"y\"\n"
            + "  on \"x\".\"deptno\"=\"y\".\"deptno\"\n";
    checkMaterialize(sql, sql);
  }

  @Test public void testUnionAllToUnionAll() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    checkMaterialize(sql0 + " union all " + sql1, sql1 + " union all " + sql0);
  }

  @Test public void testUnionDistinctToUnionDistinct() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    checkMaterialize(sql0 + " union " + sql1, sql1 + " union " + sql0);
  }

  @Test public void testUnionDistinctToUnionAll() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    checkNoMaterialize(sql0 + " union " + sql1, sql0 + " union all " + sql1);
  }

  @Test public void testUnionOnCalcsToUnion() {
    String mv = ""
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300\n"
        + "union all\n"
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100";
    String query = ""
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300 and \"salary\" > 100\n"
        + "union all\n"
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100 and \"salary\" > 100";
    checkMaterialize(mv, query);
  }

  /** Unit test for logic functions
   * {@link org.apache.calcite.plan.SubstitutionVisitor#mayBeSatisfiable} and
   * {@link RexUtil#simplify}. */
  @Test public void testSatisfiable() {
    // TRUE may be satisfiable
    checkSatisfiable(rexBuilder.makeLiteral(true), "true");

    // FALSE is not satisfiable
    checkNotSatisfiable(rexBuilder.makeLiteral(false));

    // The expression "$0 = 1".
    final RexNode i0_eq_0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                typeFactory.createType(int.class), 0),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));

    // "$0 = 1" may be satisfiable
    checkSatisfiable(i0_eq_0, "=($0, 0)");

    // "$0 = 1 AND TRUE" may be satisfiable
    final RexNode e0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeLiteral(true));
    checkSatisfiable(e0, "=($0, 0)");

    // "$0 = 1 AND FALSE" is not satisfiable
    final RexNode e1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeLiteral(false));
    checkNotSatisfiable(e1);

    // "$0 = 0 AND NOT $0 = 0" is not satisfiable
    final RexNode e2 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                i0_eq_0));
    checkNotSatisfiable(e2);

    // "TRUE AND NOT $0 = 0" may be satisfiable. Can simplify.
    final RexNode e3 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeLiteral(true),
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                i0_eq_0));
    checkSatisfiable(e3, "<>($0, 0)");

    // The expression "$1 = 1".
    final RexNode i1_eq_1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                typeFactory.createType(int.class), 1),
            rexBuilder.makeExactLiteral(BigDecimal.ONE));

    // "$0 = 0 AND $1 = 1 AND NOT $0 = 0" is not satisfiable
    final RexNode e4 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                i1_eq_1,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT, i0_eq_0)));
    checkNotSatisfiable(e4);

    // "$0 = 0 AND NOT $1 = 1" may be satisfiable. Can't simplify.
    final RexNode e5 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                i1_eq_1));
    checkSatisfiable(e5, "AND(=($0, 0), <>($1, 1))");

    // "$0 = 0 AND NOT ($0 = 0 AND $1 = 1)" may be satisfiable. Can simplify.
    final RexNode e6 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    i0_eq_0,
                    i1_eq_1)));
    checkSatisfiable(e6, "AND(=($0, 0), OR(<>($0, 0), <>($1, 1)))");

    // "$0 = 0 AND ($1 = 1 AND NOT ($0 = 0))" is not satisfiable.
    final RexNode e7 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                i1_eq_1,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT,
                    i0_eq_0)));
    checkNotSatisfiable(e7);

    // The expression "$2".
    final RexInputRef i2 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 2);

    // The expression "$3".
    final RexInputRef i3 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 3);

    // The expression "$4".
    final RexInputRef i4 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 4);

    // "$0 = 0 AND $2 AND $3 AND NOT ($2 AND $3 AND $4) AND NOT ($2 AND $4)" may
    // be satisfiable. Can't simplify.
    final RexNode e8 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                i2,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    i3,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.NOT,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.AND,
                            i2,
                            i3,
                            i4)),
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.NOT,
                        i4))));
    checkSatisfiable(e8,
        "AND(=($0, 0), $2, $3, OR(NOT($2), NOT($3), NOT($4)), NOT($4))");
  }

  private void checkNotSatisfiable(RexNode e) {
    assertFalse(SubstitutionVisitor.mayBeSatisfiable(e));
    final RexNode simple = simplify.simplifyUnknownAsFalse(e);
    assertFalse(RexLiteral.booleanValue(simple));
  }

  private void checkSatisfiable(RexNode e, String s) {
    assertTrue(SubstitutionVisitor.mayBeSatisfiable(e));
    final RexNode simple = simplify.simplifyUnknownAsFalse(e);
    assertEquals(s, simple.toString());
  }

  @Test public void testSplitFilter() {
    final RexLiteral i1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral i2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));
    final RexLiteral i3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(3));

    final RelDataType intType = typeFactory.createType(int.class);
    final RexInputRef x = rexBuilder.makeInputRef(intType, 0); // $0
    final RexInputRef y = rexBuilder.makeInputRef(intType, 1); // $1
    final RexInputRef z = rexBuilder.makeInputRef(intType, 2); // $2

    final RexNode x_eq_1 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, i1); // $0 = 1
    final RexNode x_eq_1_b =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, i1, x); // 1 = $0
    final RexNode x_eq_2 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, i2); // $0 = 2
    final RexNode y_eq_2 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, y, i2); // $1 = 2
    final RexNode z_eq_3 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, z, i3); // $2 = 3

    RexNode newFilter;

    // Example 1.
    //   condition: x = 1 or y = 2
    //   target:    y = 2 or 1 = x
    // yields
    //   residue:   true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.OR, y_eq_2, x_eq_1_b));
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // Example 2.
    //   condition: x = 1,
    //   target:    x = 1 or z = 3
    // yields
    //   residue:   x = 1
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, z_eq_3));
    assertThat(newFilter.toString(), equalTo("=($0, 1)"));

    // 2b.
    //   condition: x = 1 or y = 2
    //   target:    x = 1 or y = 2 or z = 3
    // yields
    //   residue:   x = 1 or y = 2
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2, z_eq_3));
    assertThat(newFilter.toString(), equalTo("OR(=($0, 1), =($1, 2))"));

    // 2c.
    //   condition: x = 1
    //   target:    x = 1 or y = 2 or z = 3
    // yields
    //   residue:   x = 1
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2, z_eq_3));
    assertThat(newFilter.toString(),
        equalTo("=($0, 1)"));

    // 2d.
    //   condition: x = 1 or y = 2
    //   target:    y = 2 or x = 1
    // yields
    //   residue:   true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.OR, y_eq_2, x_eq_1));
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // 2e.
    //   condition: x = 1
    //   target:    x = 1 (different object)
    // yields
    //   residue:   true
    newFilter = SubstitutionVisitor.splitFilter(simplify, x_eq_1, x_eq_1_b);
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // 2f.
    //   condition: x = 1 or y = 2
    //   target:    x = 1
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        x_eq_1);
    assertNull(newFilter);

    // Example 3.
    // Condition [x = 1 and y = 2],
    // target [y = 2 and x = 1] yields
    // residue [true].
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.AND, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.AND, y_eq_2, x_eq_1));
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // Example 4.
    //   condition: x = 1 and y = 2
    //   target:    y = 2
    // yields
    //   residue:   x = 1
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.AND, x_eq_1, y_eq_2),
        y_eq_2);
    assertThat(newFilter.toString(), equalTo("=($0, 1)"));

    // Example 5.
    //   condition: x = 1
    //   target:    x = 1 and y = 2
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        rexBuilder.makeCall(SqlStdOperatorTable.AND, x_eq_1, y_eq_2));
    assertNull(newFilter);

    // Example 6.
    //   condition: x = 1
    //   target:    y = 2
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        y_eq_2);
    assertNull(newFilter);

    // Example 7.
    //   condition: x = 1
    //   target:    x = 2
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        x_eq_2);
    assertNull(newFilter);
  }



  /** Tests a complicated star-join query on a complicated materialized
   * star-join query. Some of the features:
   *
   * <ol>
   * <li>query joins in different order;
   * <li>query's join conditions are in where clause;
   * <li>query does not use all join tables (safe to omit them because they are
   *    many-to-mandatory-one joins);
   * <li>query is at higher granularity, therefore needs to roll up;
   * <li>query has a condition on one of the materialization's grouping columns.
   * </ol>
   */
  @Ignore
  @Test public void testFilterGroupQueryOnStar() {
    TestConfig testConfig = new TestConfigBuilder()
        .defaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .materialization(
            "select p.\"product_name\", t.\"the_year\",\n"
                + "  sum(f.\"unit_sales\") as \"sum_unit_sales\", count(*) as \"c\"\n"
                + "from \"foodmart\".\"sales_fact_1997\" as f\n"
                + "join (\n"
                + "    select \"time_id\", \"the_year\", \"the_month\"\n"
                + "    from \"foodmart\".\"time_by_day\") as t\n"
                + "  on f.\"time_id\" = t.\"time_id\"\n"
                + "join \"foodmart\".\"product\" as p\n"
                + "  on f.\"product_id\" = p.\"product_id\"\n"
                + "join \"foodmart\".\"product_class\" as pc"
                + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
                + "group by t.\"the_year\",\n"
                + " t.\"the_month\",\n"
                + " pc.\"product_department\",\n"
                + " pc.\"product_category\",\n"
                + " p.\"product_name\"")
        .query(
            "select t.\"the_month\", count(*) as x\n"
                + "from (\n"
                + "  select \"time_id\", \"the_year\", \"the_month\"\n"
                + "  from \"foodmart\".\"time_by_day\") as t,\n"
                + " \"foodmart\".\"sales_fact_1997\" as f\n"
                + "where t.\"the_year\" = 1997\n"
                + "and t.\"time_id\" = f.\"time_id\"\n"
                + "group by t.\"the_year\",\n"
                + " t.\"the_month\"\n")
        .build();
    checkMaterialize(testConfig);
  }

  /** Simpler than {@link #testFilterGroupQueryOnStar()}, tests a query on a
   * materialization that is just a join. */
  @Ignore
  @Test public void testQueryOnStar() {
    String q = "select *\n"
        + "from \"foodmart\".\"sales_fact_1997\" as f\n"
        + "join \"foodmart\".\"time_by_day\" as t on f.\"time_id\" = t.\"time_id\"\n"
        + "join \"foodmart\".\"product\" as p on f.\"product_id\" = p.\"product_id\"\n"
        + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"\n";
    TestConfig testConfig = new TestConfigBuilder()
        .materialization(q)
        .query(q + "where t.\"month_of_year\" = 10")
        .defaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .build();
    checkMaterialize(testConfig);
  }

  /** A materialization that is a join of a union cannot at present be converted
   * to a star table and therefore cannot be recognized. This test checks that
   * nothing unpleasant happens. */
  @Ignore
  @Test public void testJoinOnUnionMaterialization() {
    String q = "select *\n"
        + "from (select * from \"emps\" union all select * from \"emps\")\n"
        + "join \"depts\" using (\"deptno\")";
    checkNoMaterialize(q, q);
  }

  @Ignore
  @Test public void testDifferentColumnNames() {}

  @Ignore
  @Test public void testDifferentType() {}

  @Ignore
  @Test public void testPartialUnion() {}

  @Ignore
  @Test public void testNonDisjointUnion() {}

  @Ignore
  @Test public void testMaterializationReferencesTableInOtherSchema() {}

  @Ignore
  @Test public void testOrderByQueryOnProjectView() {
    checkMaterialize(
        "select \"deptno\", \"empid\" from \"emps\"",
        "select \"empid\" from \"emps\" order by \"deptno\"");
  }

  @Ignore
  @Test public void testOrderByQueryOnOrderByView() {
    checkMaterialize(
        "select \"deptno\", \"empid\" from \"emps\" order by \"deptno\"",
        "select \"empid\" from \"emps\" order by \"deptno\"");
  }

  final JavaTypeFactoryImpl typeFactory =
      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RexSimplify simplify =
      new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
          .withParanoid(true);

  protected List<RelNode> optimize(TestConfig testConfig) {
    RelNode queryRel = testConfig.queryRel;
    RelOptMaterialization materialization = testConfig.materializations.get(0);
    List<RelNode> substitutes =
        new SubstitutionVisitor(canonicalize(materialization.queryRel), canonicalize(queryRel))
            .go(materialization.tableRel);
    return substitutes;
  }

  private RelNode canonicalize(RelNode rel) {
    HepProgram program =
        new HepProgramBuilder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterMergeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(FilterJoinRule.JOIN)
            .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
            .addRuleInstance(FilterToCalcRule.INSTANCE)
            .addRuleInstance(ProjectToCalcRule.INSTANCE)
            .addRuleInstance(FilterCalcMergeRule.INSTANCE)
            .addRuleInstance(ProjectCalcMergeRule.INSTANCE)
            .addRuleInstance(CalcMergeRule.INSTANCE)
            .build();
    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(rel);
    return hepPlanner.findBestExp();
  }
}

// End MaterializedViewSubstitutionVisitorTest.java
