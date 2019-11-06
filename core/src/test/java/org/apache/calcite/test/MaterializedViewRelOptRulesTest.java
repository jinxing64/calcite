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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Programs;

import com.google.common.collect.ImmutableList;

import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 * Unit test for extensions of AbstractMaterializedViewRule,
 * in which materialized view gets matched by using structual information of plan.
 */
public class MaterializedViewRelOptRulesTest extends AbstractMaterializedViewTest {

  @Test public void testSwapJoin() {
    TestConfig testConfig = new TestConfigBuilder()
        .defaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .materialization("select count(*) as c from \"foodmart\".\"sales_fact_1997\" as s"
            + " join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\"")
        .query("select count(*) as c from \"foodmart\".\"time_by_day\" as t"
            + " join \"foodmart\".\"sales_fact_1997\" as s on t.\"time_id\" = s.\"time_id\"")
        .build();
    checkMaterialize(testConfig);
  }

  /** Aggregation materialization with a project. */
  @Test public void testAggregateProject() {
    // Note that materialization does not start with the GROUP BY columns.
    // Not a smart way to design a materialization, but people may do it.
    checkMaterialize(
        "select \"deptno\", count(*) as c, \"empid\" + 2, sum(\"empid\") as s from \"emps\" group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c, \"deptno\" from \"emps\" group by \"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], $f0=[$t3], deptno=[$t0])\n"
            + "  EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs1() {
    checkMaterialize(
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"");
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs2() {
    checkMaterialize(
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs3() {
    checkNoMaterialize(
        "select \"deptno\" from \"emps\" group by \"deptno\"",
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"");
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs4() {
    checkMaterialize(
        "select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" = 10 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" = 10 group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs5() {
    checkNoMaterialize(
        "select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" = 5 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" = 10 group by \"deptno\"");
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs6() {
    checkMaterialize(
        "select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" > 5 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" > 10 group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[10], expr#3=[<($t2, $t1)], proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs7() {
    checkNoMaterialize(
        "select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" > 5 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" < 10 group by \"deptno\"");
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs8() {
    checkNoMaterialize(
        "select \"empid\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"");
  }

  @Test public void testAggregateMaterializationNoAggregateFuncs9() {
    checkNoMaterialize(
        "select \"empid\", \"deptno\" from \"emps\"\n"
            + "where \"salary\" > 1000 group by \"name\", \"empid\", \"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "where \"salary\" > 2000 group by \"name\", \"empid\"");
  }

  @Test public void testAggregateMaterializationAggregateFuncs1() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationAggregateFuncs2() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}], C=[$SUM0($2)], S=[$SUM0($3)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationAggregateFuncs3() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\", \"empid\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        resultContains(""
           + "EnumerableCalc(expr#0..3=[{inputs}], deptno=[$t1], empid=[$t0], S=[$t3], C=[$t2])\n"
           + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationAggregateFuncs4() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}], S=[$SUM0($3)])\n"
            + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationAggregateFuncs5() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)],"
            + " deptno=[$t0], $f1=[$t3])\n"
            + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testAggregateMaterializationAggregateFuncs6() {
    checkNoMaterialize(
        "select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") + 2 as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"");
  }

  @Test public void testAggregateMaterializationAggregateFuncs7() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\" + 1, sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t0, $t2)], "
            + "expr#4=[+($t1, $t2)], $f0=[$t3], $f1=[$t4])\n"
            + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Ignore
  @Test public void testAggregateMaterializationAggregateFuncs8() {
    // TODO: It should work, but top project in the query is not matched by the planner.
    // It needs further checking.
    checkMaterialize(
        "select \"empid\", \"deptno\" + 1, count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\" + 1, sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"");
  }

  @Test public void testAggregateMaterializationAggregateFuncs9() {
    checkMaterialize(
        "select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month), count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs10() {
    checkMaterialize(
        "select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month), count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(\"empid\") + 1 as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs11() {
    checkMaterialize(
        "select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second), count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to minute), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to minute)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs12() {
    checkMaterialize(
        "select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second), count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to month), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to month)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs13() {
    checkMaterialize(
        "select \"empid\", cast('1997-01-20 12:34:56' as timestamp), count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", cast('1997-01-20 12:34:56' as timestamp)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs14() {
    checkMaterialize(
        "select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month), count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to hour), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to hour)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs15() {
    checkMaterialize(
        "select \"eventid\", floor(cast(\"ts\" as timestamp) to second), count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", floor(cast(\"ts\" as timestamp) to second)",
        "select floor(cast(\"ts\" as timestamp) to minute), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to minute)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs16() {
    checkMaterialize(
        "select \"eventid\", cast(\"ts\" as timestamp), count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", cast(\"ts\" as timestamp)",
        "select floor(cast(\"ts\" as timestamp) to year), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to year)");
  }

  @Test public void testAggregateMaterializationAggregateFuncs17() {
    checkMaterialize(
        "select \"eventid\", floor(cast(\"ts\" as timestamp) to month), count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", floor(cast(\"ts\" as timestamp) to month)",
        "select floor(cast(\"ts\" as timestamp) to hour), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to hour)",
        resultContains("EnumerableTableScan(table=[[hr, events]])"));
  }

  @Test public void testAggregateMaterializationAggregateFuncs18() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\"*\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\"*\"deptno\"");
  }

  @Test public void testAggregateMaterializationAggregateFuncs19() {
    checkMaterialize(
        "select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\" + 10, count(*) + 1 as c\n"
            + "from \"emps\" group by \"empid\" + 10");
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs1() {
    checkMaterialize(
        "select \"empid\", \"depts\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t1)], "
            + "empid=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs2() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t0)], "
            + "empid=[$t1], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs3() {
    // It does not match, Project on top of query
    checkNoMaterialize(
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"");
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs4() {
    checkMaterialize(
        "select \"empid\", \"depts\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"emps\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t1)], "
            + "empid=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs5() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"emps\".\"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 10\n"
            + "group by \"depts\".\"deptno\", \"emps\".\"empid\"",
        "select \"depts\".\"deptno\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 15\n"
            + "group by \"depts\".\"deptno\", \"emps\".\"empid\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[15], expr#3=[<($t2, $t1)], "
            + "deptno=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs6() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"emps\".\"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 10\n"
            + "group by \"depts\".\"deptno\", \"emps\".\"empid\"",
        "select \"depts\".\"deptno\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 15\n"
            + "group by \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{0}])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[15], expr#3=[<($t2, $t1)], "
            + "proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs7() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10\n"
            + "group by \"dependents\".\"empid\"",
        resultContains(
            "EnumerableAggregate(group=[{0}])",
            "EnumerableUnion(all=[true])",
            "EnumerableAggregate(group=[{2}])",
            "EnumerableTableScan(table=[[hr, MV0]])",
            "expr#5=[10], expr#6=[>($t0, $t5)], expr#7=[11], expr#8=[>=($t7, $t0)]"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs8() {
    checkNoMaterialize(
        "select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 20\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"");
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs9() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"",
        resultContains(
            "EnumerableAggregate(group=[{0}])",
            "EnumerableUnion(all=[true])",
            "EnumerableAggregate(group=[{2}])",
            "EnumerableTableScan(table=[[hr, MV0]])",
            "expr#13=[OR($t10, $t12)], expr#14=[AND($t6, $t8, $t13)]"));
  }

  @Test public void testJoinAggregateMaterializationNoAggregateFuncs10() {
    checkMaterialize(
        "select \"depts\".\"name\", \"dependents\".\"name\" as \"name2\", "
            + "\"emps\".\"deptno\", \"depts\".\"deptno\" as \"deptno2\", "
            + "\"dependents\".\"empid\"\n"
            + "from \"depts\", \"dependents\", \"emps\"\n"
            + "where \"depts\".\"deptno\" > 10\n"
            + "group by \"depts\".\"name\", \"dependents\".\"name\", "
            + "\"emps\".\"deptno\", \"depts\".\"deptno\", "
            + "\"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10\n"
            + "group by \"dependents\".\"empid\"",
        resultContains(""
            + "EnumerableAggregate(group=[{4}])\n"
            + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[=($t2, $t3)], "
            + "expr#6=[CAST($t1):VARCHAR], "
            + "expr#7=[CAST($t0):VARCHAR], "
            + "expr#8=[=($t6, $t7)], expr#9=[AND($t5, $t8)], proj#0..4=[{exprs}], $condition=[$t9])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs1() {
    // This test relies on FK-UK relationship
    checkMaterialize(
        "select \"empid\", \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs2() {
    checkMaterialize(
        "select \"empid\", \"emps\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}], C=[$SUM0($2)], S=[$SUM0($3)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs3() {
    // This test relies on FK-UK relationship
    checkMaterialize(
        "select \"empid\", \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"deptno\", \"empid\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], deptno=[$t1], empid=[$t0], S=[$t3], C=[$t2])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs4() {
    checkMaterialize(
        "select \"empid\", \"emps\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"emps\".\"deptno\" >= 10 group by \"empid\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"emps\".\"deptno\" > 10 group by \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{1}], S=[$SUM0($3)])\n"
            + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs5() {
    checkMaterialize(
        "select \"empid\", \"depts\".\"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"deptno\" >= 10 group by \"empid\", \"depts\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 group by \"depts\".\"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], "
            + "deptno=[$t0], S=[$t3])\n"
            + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Ignore
  @Test public void testJoinAggregateMaterializationAggregateFuncs6() {
    // This rewriting would be possible if planner generates a pre-aggregation,
    // since the materialized view would match the sub-query.
    // Initial investigation after enabling AggregateJoinTransposeRule.EXTENDED
    // shows that the rewriting with pre-aggregations is generated and the
    // materialized view rewriting happens.
    // However, we end up discarding the plan with the materialized view and still
    // using the plan with the pre-aggregations.
    // TODO: Explore and extend to choose best rewriting.
    final String m = "select \"depts\".\"name\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "group by \"depts\".\"name\"";
    final String q = "select \"dependents\".\"empid\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "group by \"dependents\".\"empid\"";
    checkMaterialize(m, q);
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs7() {
    checkMaterialize(
        "select \"dependents\".\"empid\", \"emps\".\"deptno\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"dependents\".\"empid\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\"",
        resultContains(""
            + "EnumerableAggregate(group=[{0}], S=[$SUM0($2)])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    EnumerableTableScan(table=[[hr, depts]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs8() {
    checkMaterialize(
        "select \"dependents\".\"empid\", \"emps\".\"deptno\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"depts\".\"name\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"depts\".\"name\"",
        resultContains(""
            + "EnumerableAggregate(group=[{4}], S=[$SUM0($2)])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    EnumerableTableScan(table=[[hr, depts]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs9() {
    checkMaterialize(
        "select \"dependents\".\"empid\", \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        resultContains(""
            + "EnumerableCalc(expr#0..2=[{inputs}], deptno=[$t1], S=[$t2])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs10() {
    checkNoMaterialize(
        "select \"dependents\".\"empid\", \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"emps\".\"deptno\"");
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs11() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"dependents\".\"empid\", count(\"emps\".\"salary\") as s\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\", count(\"emps\".\"salary\") + 1\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"",
        resultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], "
                + "empid=[$t0], EXPR$1=[$t3])\n"
                + "  EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)])",
            "EnumerableUnion(all=[true])",
            "EnumerableAggregate(group=[{2}], agg#0=[COUNT()])",
            "EnumerableAggregate(group=[{1}], agg#0=[$SUM0($2)])",
            "EnumerableTableScan(table=[[hr, MV0]])",
            "expr#13=[OR($t10, $t12)], expr#14=[AND($t6, $t8, $t13)]"));
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs12() {
    checkNoMaterialize(
        "select \"depts\".\"deptno\", \"dependents\".\"empid\", count(distinct \"emps\".\"salary\") as s\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\", count(distinct \"emps\".\"salary\") + 1\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"");
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs13() {
    checkNoMaterialize(
        "select \"dependents\".\"empid\", \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"emps\".\"deptno\", count(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"");
  }

  @Test public void testJoinAggregateMaterializationAggregateFuncs14() {
    checkMaterialize(
        "select \"empid\", \"emps\".\"name\", \"emps\".\"deptno\", \"depts\".\"name\", "
            + "count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b')\n"
            + "group by \"empid\", \"emps\".\"name\", \"depts\".\"name\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"name\" is not null and \"emps\".\"name\" = 'a'\n"
            + "group by \"depts\".\"deptno\"");
  }

  @Test public void testJoinMaterialization1() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join \"depts\" using (\"deptno\")";
    checkMaterialize("select * from \"emps\" where \"empid\" < 500", q);
  }

  @Ignore
  @Test public void testJoinMaterialization2() {
    String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    final String m = "select \"deptno\", \"empid\", \"name\",\n"
        + "\"salary\", \"commission\" from \"emps\"";
    checkMaterialize(m, q);
  }

  @Test public void testJoinMaterialization3() {
    String q = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" = 1";
    final String m = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    checkMaterialize(m, q);
  }

  @Test public void testJoinMaterialization4() {
    checkMaterialize(
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1",
        resultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):INTEGER NOT NULL], expr#2=[1], "
            + "expr#3=[=($t1, $t2)], deptno=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinMaterialization5() {
    checkMaterialize(
        "select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" > 1",
        resultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
            + "expr#2=[1], expr#3=[>($t1, $t2)], EXPR$0=[$t1], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinMaterialization6() {
    checkMaterialize(
        "select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1",
        resultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
            + "expr#2=[CAST($t1):INTEGER NOT NULL], expr#3=[1], expr#4=[=($t2, $t3)], "
            + "EXPR$0=[$t1], $condition=[$t4])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinMaterialization7() {
    checkMaterialize(
        "select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")",
        resultContains(""
            + "EnumerableCalc(expr#0..2=[{inputs}], empid=[$t1])\n"
            + "  EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], name=[$t1])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t1):VARCHAR], empid=[$t0], name0=[$t2])\n"
            + "      EnumerableTableScan(table=[[hr, dependents]])"));
  }

  @Test public void testJoinMaterialization8() {
    checkMaterialize(
        "select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        resultContains(""
            + "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t2])\n"
            + "  EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t1):VARCHAR], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[hr, dependents]])"));
  }

  @Test public void testJoinMaterialization9() {
    checkMaterialize(
        "select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")");
  }

  @Test public void testJoinMaterialization10() {
    checkMaterialize(
        "select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 30",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10",
        resultContains(
            "EnumerableUnion(all=[true])",
            "EnumerableTableScan(table=[[hr, MV0]])",
            "expr#5=[10], expr#6=[>($t0, $t5)], expr#7=[30], expr#8=[>=($t7, $t0)]"));
  }

  @Test public void testJoinMaterialization11() {
    checkMaterialize(
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\"\n"
            + "where \"deptno\" in (select \"deptno\" from \"depts\")");
  }

  @Test public void testJoinMaterialization12() {
    checkMaterialize(
        "select \"empid\", \"emps\".\"name\", \"emps\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'c')",
        "select \"depts\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b')");
  }

  @Test public void testJoinMaterializationUKFK1() {
    checkMaterialize(
        "select \"a\".\"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from \n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")");
  }

  @Test public void testJoinMaterializationUKFK2() {
    checkMaterialize(
        "select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from \n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinMaterializationUKFK3() {
    checkNoMaterialize(
        "select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"name\" from \n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n");
  }

  @Test public void testJoinMaterializationUKFK4() {
    checkMaterialize(
        "select \"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1)\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\" where \"empid\" = 1\n");
  }

  @Test public void testJoinMaterializationUKFK5() {
    checkMaterialize(
        "select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], empid0=[$t0])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinMaterializationUKFK6() {
    checkMaterialize(
        "select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"deptno\"=\"b\".\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1",
        resultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], empid0=[$t0])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])"));
  }

  @Test public void testJoinMaterializationUKFK7() {
    checkNoMaterialize(
        "select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"name\"=\"a\".\"name\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"name\"=\"b\".\"name\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1");
  }

  @Test public void testJoinMaterializationUKFK8() {
    checkNoMaterialize(
        "select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"name\"=\"b\".\"name\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1");
  }

  @Test public void testJoinMaterializationUKFK9() {
    checkMaterialize(
        "select * from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"emps\".\"empid\", \"dependents\".\"empid\", \"emps\".\"deptno\"\n"
            + "from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "where \"emps\".\"name\" = 'Bill'");
  }


  @Test public void testAggregateMaterializationOnCountDistinctQuery1() {
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    checkMaterialize(
        "select \"deptno\", \"empid\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"deptno\", count(distinct \"empid\") as c from (\n"
            + "select \"deptno\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\")\n"
            + "group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{0}], C=[COUNT($1)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]]"));
  }

  @Test public void testAggregateMaterializationOnCountDistinctQuery2() {
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    checkMaterialize(
        "select \"deptno\", \"salary\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\", \"empid\"",
        "select \"deptno\", count(distinct \"empid\") as c from (\n"
            + "select \"deptno\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\")\n"
            + "group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{0}], C=[COUNT($2)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]]"));
  }

  @Test public void testAggregateMaterializationOnCountDistinctQuery3() {
    // The column salary is not unique, thus we end up with
    // a different rewriting
    checkMaterialize(
        "select \"deptno\", \"empid\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"deptno\", count(distinct \"salary\") from (\n"
            + "select \"deptno\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\")\n"
            + "group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
            + "  EnumerableAggregate(group=[{0, 2}])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]]"));
  }

  @Test public void testAggregateMaterializationOnCountDistinctQuery4() {
    // Although there is no DISTINCT in the COUNT, this is
    // equivalent to previous query
    checkMaterialize(
        "select \"deptno\", \"salary\", \"empid\"\n"
          + "from \"emps\"\n"
          + "group by \"deptno\", \"salary\", \"empid\"",
        "select \"deptno\", count(\"salary\") from (\n"
            + "select \"deptno\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\")\n"
            + "group by \"deptno\"",
        resultContains(""
            + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])\n"
            + "  EnumerableAggregate(group=[{0, 1}])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]]"));
  }


  protected List<RelNode> optimize(TestConfig testConfig) {
    RelNode queryRel = testConfig.queryRel;
    RelOptPlanner planner = queryRel.getCluster().getPlanner();
    RelTraitSet traitSet = queryRel.getCluster().traitSet()
        .replace(EnumerableConvention.INSTANCE);
    RelOptUtil.registerDefaultRules(planner, true, false);
    return ImmutableList.of(
        Programs.standard().run(
            planner, queryRel, traitSet, testConfig.materializations, ImmutableList.of()));
  }
}

// End MaterializedViewRelOptRulesTest.java
