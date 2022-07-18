/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;

import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFlinkTableSinkRowTimeFilter extends FlinkTestBase {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String CATALOG_TYPE = "hadoop";
  private static final String CATALOG_IMPL = HadoopCatalog.class.getName();
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String TIME_ZONE = "UTC";

  private static final FileFormat format = FileFormat.PARQUET;
  private static String warehouse;
  private Catalog catalog;
  private org.apache.iceberg.Table icebergTable;

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(TableConfigOptions.LOCAL_TIME_ZONE, TIME_ZONE)
        .set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='%s', 'warehouse'='%s')", CATALOG_NAME,
        CATALOG_TYPE, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);

    catalog = CatalogUtil.loadCatalog(CATALOG_IMPL, CATALOG_NAME,
        Collections.singletonMap(CatalogProperties.WAREHOUSE_LOCATION, warehouse), hiveConf);
    TableIdentifier tableIdentifier = TableIdentifier.of(DATABASE_NAME, TABLE_NAME);
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).hour("ts").build();
    Map<String, String> tableProperties =
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, format.name(),
            TableProperties.FORMAT_VERSION, "2");
    icebergTable = catalog.createTable(tableIdentifier, schema, partitionSpec, tableProperties);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    sql("DROP CATALOG IF EXISTS %s", CATALOG_NAME);
  }

  @Test
  public void testSinkStartRowTimeFilter() {
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651338000000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);

    Table sourceTable = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(1,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))," +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651338000)))," +
        "(3,TO_TIMESTAMP(FROM_UNIXTIME(1651341600)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTable", sourceTable);
    sql("INSERT INTO default_catalog.default_database.%s SELECT id, ts from sourceTable", TABLE_NAME);

    icebergTable.refresh();
    GenericRecord record = GenericRecord.create(icebergTable.schema());
    List<Record> expected = Arrays.asList(
        record.copy("id", 2, "ts", Instant.ofEpochSecond(1651338000).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()),
        record.copy("id", 3, "ts", Instant.ofEpochSecond(1651341600).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    List<Record> actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSinkEndRowTimeFilter() {
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.endRowTime'='1651338000000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);

    Table sourceTable = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(1,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))," +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651338000)))," +
        "(3,TO_TIMESTAMP(FROM_UNIXTIME(1651341600)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTable", sourceTable);
    sql("INSERT INTO default_catalog.default_database.%s SELECT id, ts from sourceTable", TABLE_NAME);

    icebergTable.refresh();
    GenericRecord record = GenericRecord.create(icebergTable.schema());
    List<Record> expected = Arrays.asList(
        record.copy("id", 1, "ts", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    List<Record> actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSinkRowTimeFilter() {
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651338000000000'," +
            "'sink.endRowTime'='1651341600000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);

    Table sourceTable = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(1,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))," +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651338000)))," +
        "(3,TO_TIMESTAMP(FROM_UNIXTIME(1651341600)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTable", sourceTable);
    sql("INSERT INTO default_catalog.default_database.%s SELECT id, ts from sourceTable", TABLE_NAME);

    icebergTable.refresh();
    GenericRecord record = GenericRecord.create(icebergTable.schema());
    List<Record> expected = Arrays.asList(
        record.copy("id", 2, "ts", Instant.ofEpochSecond(1651338000).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    List<Record> actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOverwritePartitionExpression() {
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);

    Table sourceTableA = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(1,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))," +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651338000)))," +
        "(3,TO_TIMESTAMP(FROM_UNIXTIME(1651341600)))," +
        "(4,TO_TIMESTAMP(FROM_UNIXTIME(1651345200)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTableA", sourceTableA);
    sql("INSERT INTO default_catalog.default_database.%s SELECT id, ts from sourceTableA", TABLE_NAME);

    icebergTable.refresh();
    GenericRecord record = GenericRecord.create(icebergTable.schema());
    List<Record> expected = Arrays.asList(
        record.copy("id", 1, "ts", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()),
        record.copy("id", 2, "ts", Instant.ofEpochSecond(1651338000).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()),
        record.copy("id", 3, "ts", Instant.ofEpochSecond(1651341600).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()),
        record.copy("id", 4, "ts", Instant.ofEpochSecond(1651345200).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    List<Record> actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertTrue(CollectionUtils.isEqualCollection(expected, actual));

    getTableEnv().dropTemporaryTable(String.format("default_catalog.default_database.%s", TABLE_NAME));
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651338000000000'," +
            "'sink.endRowTime'='1651345200000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    Table sourceTableB = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651338001)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTableB", sourceTableB);
    sql("INSERT OVERWRITE default_catalog.default_database.%s SELECT id, ts from sourceTableB", TABLE_NAME);

    icebergTable.refresh();
    expected = Arrays.asList(
        record.copy("id", 1, "ts", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()),
        record.copy("id", 2, "ts", Instant.ofEpochSecond(1651338001).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()),
        record.copy("id", 4, "ts", Instant.ofEpochSecond(1651345200).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertTrue(CollectionUtils.isEqualCollection(expected, actual));

    getTableEnv().dropTemporaryTable(String.format("default_catalog.default_database.%s", TABLE_NAME));
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    sql("INSERT OVERWRITE default_catalog.default_database.%s SELECT id, ts from sourceTableB", TABLE_NAME);

    icebergTable.refresh();
    expected = Arrays.asList(
        record.copy("id", 2, "ts", Instant.ofEpochSecond(1651338001).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertTrue(CollectionUtils.isEqualCollection(expected, actual));
  }

  @Test
  public void testOverwriteMultiplePartitionSpec() {
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    Table sourceTableA = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(1,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTableA", sourceTableA);
    sql("INSERT INTO default_catalog.default_database.%s SELECT id, ts from sourceTableA", TABLE_NAME);

    icebergTable.refresh();
    GenericRecord record = GenericRecord.create(icebergTable.schema());
    List<Record> expected = Arrays.asList(
        record.copy("id", 1, "ts", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    List<Record> actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertTrue(CollectionUtils.isEqualCollection(expected, actual));

    icebergTable.updateSchema()
        .allowIncompatibleChanges()
        .makeColumnOptional("ts")
        .addRequiredColumn("ts_1", Types.TimestampType.withoutZone())
        .commit();
    icebergTable.updateSpec()
        .removeField(Expressions.hour("ts"))
        .addField(Expressions.hour("ts_1"))
        .commit();
    icebergTable.refresh();
    record = GenericRecord.create(icebergTable.schema());

    getTableEnv().dropTemporaryTable(String.format("default_catalog.default_database.%s", TABLE_NAME));
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6)," +
            "ts_1 TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651334400000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    Table sourceTableB = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTableB", sourceTableB);
    sql("INSERT OVERWRITE default_catalog.default_database.%s SELECT id, ts, ts as ts_1 from sourceTableB", TABLE_NAME);

    icebergTable.refresh();
    expected = Arrays.asList(
        record.copy("id", 1,
            "ts", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime(),
            "ts_1", null),
        record.copy("id", 2,
            "ts", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime(),
            "ts_1", Instant.ofEpochSecond(1651334400).atZone(ZoneId.of(TIME_ZONE)).toLocalDateTime()));
    actual = Lists.newArrayList(IcebergGenerics.read(icebergTable).build());
    Assert.assertTrue(CollectionUtils.isEqualCollection(expected, actual));
  }

  @Test
  public void testIllegalRowTimeFilter() {
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651338001000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    Table sourceTableA = getTableEnv().sqlQuery("SELECT id, ts FROM (VALUES " +
        "(1,TO_TIMESTAMP(FROM_UNIXTIME(1651334400)))," +
        "(2,TO_TIMESTAMP(FROM_UNIXTIME(1651338000)))," +
        "(3,TO_TIMESTAMP(FROM_UNIXTIME(1651341600)))" +
        ") t (id, ts)");
    getTableEnv().createTemporaryView("sourceTableA", sourceTableA);
    String sql =
        String.format("INSERT INTO default_catalog.default_database.%s SELECT id, ts from sourceTableA", TABLE_NAME);

    try {
      sql(sql);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      /* expected exception */
    }

    getTableEnv().dropTemporaryTable(String.format("default_catalog.default_database.%s", TABLE_NAME));
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.endRowTime'='1651341601000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);

    try {
      sql(sql);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      /* expected exception */
    }

    getTableEnv().dropTemporaryTable(String.format("default_catalog.default_database.%s", TABLE_NAME));
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651338000000000'," +
            "'sink.endRowTime'='1651338000000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    try {
      sql(sql);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      /* expected exception */
    }

    getTableEnv().dropTemporaryTable(String.format("default_catalog.default_database.%s", TABLE_NAME));
    sql("CREATE TEMPORARY TABLE default_catalog.default_database.%s (" +
            "id INT," +
            "ts TIMESTAMP(6) NOT NULL" +
            ") WITH (" +
            "'connector'='iceberg'," +
            "'catalog-type'='%s'," +
            "'catalog-name'='%s'," +
            "'warehouse'='%s'," +
            "'write.format.default'='%s'," +
            "'catalog-database'='%s'," +
            "'catalog-table'='%s'," +
            "'sink.startRowTime'='1651345200000000'," +
            "'sink.endRowTime'='1651338000000000')",
        TABLE_NAME, CATALOG_TYPE, CATALOG_NAME, warehouse, format.name(), DATABASE_NAME, TABLE_NAME);
    try {
      sql(sql);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      /* expected exception */
    }
  }
}
