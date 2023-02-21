package com.learning.spark.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.List;
import java.util.Arrays;

public class CustomPartitoner {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder
                ().master("local[*]").getOrCreate();
        final List<String> strings = Arrays.asList("Owl", "Parrot", "Crow", "Ibis", "WoodPecker",
                "Bulbul", "Falcon",
                "Eagle", "Kite", "Toucan");
        final Dataset<String> stringDataset = sparkSession.createDataset(strings, Encoders.STRING());
        System.out.println("before repartition :"+ stringDataset.toJavaRDD().getNumPartitions());
        Dataset<Row> stringDatasetSample= stringDataset.repartition(100).toDF();
        System.out.println("after repartition :"+stringDatasetSample.toJavaRDD().getNumPartitions());
        stringDatasetSample.write().format("csv").save("C:\\GitProject\\ComplexTypesDemo\\src\\main\\scala\\com\\learning\\spark\\examples\\Dump10");
        stringDataset.write().format("csv").save("C:\\GitProject\\ComplexTypesDemo\\src\\main\\scala\\com\\learning\\spark\\examples\\Dump100");
    }
}
