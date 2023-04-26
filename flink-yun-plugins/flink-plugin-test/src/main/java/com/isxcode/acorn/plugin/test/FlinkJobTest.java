package com.isxcode.acorn.plugin.test;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkJobTest {

  public static void main(String[] args) {

    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);
    tEnv.getConfig().getConfiguration().setString("pipeline.name", "ispong-pipeline");

    tEnv.executeSql("" +
      "CREATE CATALOG source_db WITH (\n" +
      "    'type' = 'hive',\n" +
      "    'hive-conf-dir' = '/Users/ispong/Data/hive/conf',\n" +
      "    'default-database' = 'ispong_db'\n" +
      ")");

    tEnv.executeSql("CREATE TABLE source_table(\n" +
      "    username STRING,\n" +
      "    age INT" +
      ") WITH (\n" +
      "    'connector'='jdbc',\n" +
      "    'url'='jdbc:mysql://ispong-mac.local:30306/ispong_db',\n" +
      "    'table-name'='user_source',\n" +
      "    'driver'='com.mysql.cj.jdbc.Driver',\n" +
      "    'username'='root',\n" +
      "    'password'='ispong123')");

    // replace(replace(username,'',''),'\n','')
    tEnv.executeSql("insert into source_db.sink_table select username, age from source_table").print();

  }
}

