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

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Abstract class to provide testing environment & utils for extensions.
 */
public abstract class AbstractMaterializedViewTest {

  /**
   * Abstract method to customize materialization matching approach.
   */
  protected abstract List<RelNode> optimize(TestConfig testConfig);

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  protected void checkMaterialize(String mv, String query) {
    checkMaterialize(new TestConfigBuilder().query(query).materialization(mv).build());
  }

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  protected void checkMaterialize(String mv, String query, Function<String, Boolean> matcher) {
    checkMaterialize(
        new TestConfigBuilder().query(query).materialization(mv).build(), matcher);
  }

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  protected void checkMaterialize(TestConfig testConfig) {
    checkMaterialize(testConfig,
        resultContains("EnumerableTableScan(table=[[" + testConfig.defaultSchema + ", MV0]]"));
  }

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  protected void checkMaterialize(TestConfig testConfig, Function<String, Boolean> matcher) {
    List<RelNode> substitutes = optimize(testConfig);
    if (substitutes.stream().noneMatch(sub -> matcher.apply(RelOptUtil.toString(sub)))) {
      StringBuilder substituteMessages = new StringBuilder();
      for (RelNode sub: substitutes) {
        substituteMessages.append(RelOptUtil.toString(sub) + "\n");
      }
      throw new AssertionError("Materialized view failed to be matched by optmized results:\n"
          + substituteMessages.toString());
    }
  }

  /** Checks that a given query cannot use a materialized view with a given
   * definition. */
  protected void checkNoMaterialize(String mv, String query) {
    checkNoMaterialize(new TestConfigBuilder().query(query).materialization(mv).build());
  }

  /** Checks that a given query cannot use a materialized view with a given
   * definition. */
  protected void checkNoMaterialize(TestConfig testConfig) {
    List<RelNode> results = optimize(testConfig);
    if (results.isEmpty()
        || (results.size() == 1
            && !RelOptUtil.toString(results.get(0)).contains("MV0"))) {
      return;
    }
    StringBuilder errMsgBuilder = new StringBuilder();
    errMsgBuilder.append("Optmization succeeds out of expectation: ");
    for (RelNode res: results) {
      errMsgBuilder.append(RelOptUtil.toString(res) + "\n");
    }
    throw new AssertionError(errMsgBuilder.toString());
  }

  protected Function<String, Boolean> resultContains(
      final String... expected) {
    return s -> {
      final String actual = s;
      for (String st : expected) {
        if (!Matchers.containsStringLinux(st).matches(s)) {
          return false;
        }
      }
      return true;
    };
  }

  /**
   * Testing definition builder.
   */
  protected class TestConfigBuilder {
    private CalciteAssert.SchemaSpec defaultSchemaSpec;
    private String query;
    private List<Pair<String, String>> materializationSpecs;

    public TestConfigBuilder defaultSchemaSpec(
        CalciteAssert.SchemaSpec defaultSchemaSpec) {
      this.defaultSchemaSpec = defaultSchemaSpec;
      return this;
    }

    public TestConfigBuilder query(String query) {
      this.query = query;
      return this;
    }

    public TestConfigBuilder materialization(String materialization) {
      materializations(ImmutableList.of(Pair.of(materialization, "MV0")));
      return this;
    }

    public TestConfigBuilder materializations(
        List<Pair<String, String>> materializationSpecs) {
      this.materializationSpecs = materializationSpecs;
      return this;
    }

    public TestConfig build() {
      assert query != null;
      final TestConfig result =
          Frameworks.withPlanner((cluster, relOptSchema, rootSchema) -> {
            cluster.getPlanner().setExecutor(
                new RexExecutorImpl(Schemas.createDataContext(null, null)));
            try {
              final SchemaPlus defaultSchema;
              if (defaultSchemaSpec == null) {
                defaultSchema = rootSchema.add("hr",
                    new ReflectiveSchema(new MaterializationTest.HrFKUKSchema()));
              } else {
                defaultSchema = CalciteAssert.addSchema(rootSchema, defaultSchemaSpec);
              }
              final RelNode queryRel = toRel(cluster, rootSchema, defaultSchema, query);
              final List<RelOptMaterialization> mvs = new ArrayList<>();
              final RelBuilder relBuilder =
                  RelFactories.LOGICAL_BUILDER.create(cluster, relOptSchema);
              final MaterializationService.DefaultTableFactory tableFactory =
                  new MaterializationService.DefaultTableFactory();
              for (Pair<String, String> pair: materializationSpecs) {
                final RelNode mvRel = toRel(cluster, rootSchema, defaultSchema, pair.left);
                final Table table = tableFactory.createTable(CalciteSchema.from(rootSchema),
                    pair.left, ImmutableList.of(defaultSchema.getName()));
                defaultSchema.add(pair.right, table);
                relBuilder.scan(defaultSchema.getName(), pair.right);
                final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
                final EnumerableTableScan replacement =
                    EnumerableTableScan.create(cluster, logicalScan.getTable());
                mvs.add(
                    new RelOptMaterialization(replacement, mvRel, null,
                        ImmutableList.of(defaultSchema.getName(), pair.right)));
              }
              return new TestConfig(defaultSchema.getName(), queryRel, mvs);
            } catch (Exception e) {
              throw TestUtil.rethrow(e);
            }
          });
      return result;
    }

    private RelNode toRel(RelOptCluster cluster, SchemaPlus rootSchema,
        SchemaPlus defaultSchema, String sql) throws SqlParseException {
      final SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
      final SqlNode parsed = parser.parseStmt();

      final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
          CalciteSchema.from(rootSchema),
          CalciteSchema.from(defaultSchema).path(null),
          new JavaTypeFactoryImpl(), new CalciteConnectionConfigImpl(new Properties()));

      final SqlValidator validator = new ValidatorForTest(SqlStdOperatorTable.instance(),
          catalogReader, new JavaTypeFactoryImpl(), SqlConformanceEnum.DEFAULT);
      final SqlNode validated = validator.validate(parsed);
      final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
          .withTrimUnusedFields(true)
          .withExpand(true)
          .withDecorrelationEnabled(true)
          .build();
      final SqlToRelConverter converter = new SqlToRelConverter(
          (rowType, queryString, schemaPath, viewPath) -> {
            throw new UnsupportedOperationException("cannot expand view");
          }, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
      return converter.convertQuery(validated, false, true).rel;
    }

    /** Validator for testing. */
    private class ValidatorForTest extends SqlValidatorImpl {
      ValidatorForTest(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
          RelDataTypeFactory typeFactory, SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
      }
    }
  }

  /**
   * Testing definition.
   */
  protected class TestConfig {
    public final String defaultSchema;
    public final RelNode queryRel;
    public final List<RelOptMaterialization> materializations;

    public TestConfig(String defaultSchema, RelNode queryRel,
        List<RelOptMaterialization> materializations) {
      this.defaultSchema = defaultSchema;
      this.queryRel = queryRel;
      this.materializations = materializations;
    }
  }
}

// End AbstractMaterializedViewTest.java
