package com.learning.spark.examples;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class CsvToHive {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder().appName("CsvToHive").enableHiveSupport().getOrCreate();

        // Read the CSV file into a Dataset
        Dataset<Row> data = spark.read().format("csv").option("header", "true").load("path/to/data.csv");

        // Write the Dataset to a Hive table
        data.write().format("hive").saveAsTable("mydatabase.mytable");
    }
}
