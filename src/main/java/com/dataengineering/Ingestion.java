package com.dataengineering;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.dataengineering.HiveUtils.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;

public class Ingestion {

    final static Logger logger = Logger.getLogger(Ingestion.class);

    private static SparkSession InitiateSpark() {
        SparkConf conf = new SparkConf().setAppName("SparkJava").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().appName("SparkJava").config(conf).enableHiveSupport().getOrCreate();

        return spark;
    }

    public static void main(String[] args) {
        Properties prop = System.getProperties();
        String dir = prop.getProperty("user.dir");
        PropertyConfigurator.configure(dir + "/log4j.properties");
        SparkSession spark = InitiateSpark();
        ExternalCatalog externalCatalog = spark.sharedState().externalCatalog();

        Dataset<Row> mysql_data = getMySqlData(spark);
        mysql_data.printSchema();
        mysql_data.show();

        String database = args[0];
        String tablename = args[1];
        String tableLocation = HiveUtils.getHivePath(externalCatalog,database,tablename,spark);
        logger.info("HDFS location is : "+tableLocation);
    }

    private static Dataset<Row> getMySqlData(SparkSession spark) {
        logger.info("Loading data from CSV");
        Dataset<Row> mysql_data = spark.read()
                .option("header", "true")
                .option("delimiter", ",")
                .csv("hdfs://quickstart.cloudera:8020/user/cloudera/data/test_data.csv");
        logger.info("sending back csv data");
        return mysql_data;
    }

    private static List<String> ExtractColumns(Dataset<Row> mysql_data){
        List<String> ColumnsList = new ArrayList<String>();
        ColumnsList = Arrays.asList(mysql_data.columns());
        logger.info("Total columns in CSV data : ");
        System.out.println(ColumnsList);
        return ColumnsList;
    }
}