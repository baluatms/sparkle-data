package com.dataengineering;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HiveUtils {
    public static String getHivePath(ExternalCatalog externalCatalog, String db, String tableName, SparkSession spark){
        String extPath = externalCatalog.getTable(db,tableName).location().toString();
        return extPath;
    }
    public static boolean registerHivePartitions(String db,String tableName, String[] partitions){
        return false;
    }

    public static boolean writeHiveData(SparkSession spark, Dataset<Row> df){
        spark.sqlContext().setConf("hive.exec.dynamic.partition","true");
        spark.sqlContext().setConf("hive.exec.dynamic.partition.mode","nonstrict");
        df.write().format("hive").saveAsTable("");
        return false;
    }
}