package com.learning.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

package object examples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Complex Types Demo")
      .getOrCreate()

    val parqDF = spark.read.parquet("C:\\GitProject\\SparkProgrammingInScala\\22-ComplexTypesDemo\\src\\main\\scala\\guru\\learningjournal\\spark\\examples\\part-00000-deb93f20-b2d5-46b1-9183-8487b42ebfa5-c000.snappy.parquet")
    parqDF.show(false)




  }

}
