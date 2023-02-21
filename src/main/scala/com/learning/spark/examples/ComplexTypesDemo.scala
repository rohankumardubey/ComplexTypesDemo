package com.learning.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql.types._

object ComplexTypesDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  System.setProperty("hadoop.home.dir", "C:\\Spark\\winutils.exe")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Complex Types Demo")
      .getOrCreate()

//    val schema = StructType(List(
//      StructField("InvoiceNumber", StringType),
//      StructField("CreatedTime", LongType),
//      StructField("StoreID", StringType),
//      StructField("PosID", StringType),
//      StructField("CashierID", StringType),
//      StructField("CustomerType", StringType),
//      StructField("CustomerCardNo", StringType),
//      StructField("TotalAmount", DoubleType),
//      StructField("NumberOfItems", IntegerType),
//      StructField("PaymentMethod", StringType),
//      StructField("CGST", DoubleType),
//      StructField("SGST", DoubleType),
//      StructField("CESS", DoubleType),
//      StructField("DeliveryType", StringType),
//      StructField("DeliveryAddress", StructType(List(
//        StructField("AddressLine", StringType),
//        StructField("City", StringType),
//        StructField("State", StringType),
//        StructField("PinCode", StringType),
//        StructField("ContactNumber", StringType)
//      ))),
//      StructField("InvoiceLineItems", ArrayType(StructType(List(
//        StructField("ItemCode", StringType),
//        StructField("ItemDescription", StringType),
//        StructField("ItemPrice", DoubleType),
//        StructField("ItemQty", IntegerType),
//        StructField("TotalValue", DoubleType),
//      )))),
//    ))

//    val df1 = spark.read.schema(schema).json("data/")
//
//    val df2 = df1.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
//      "CustomerType", "PaymentMethod", "DeliveryType",
//      "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode",
//      "explode(InvoiceLineItems) as LineItem")
//
//    val df4 = df2.withColumn("ItemCode", expr("LineItem.ItemCode"))
//      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
//      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
//      .withColumn("ItemQty", expr("LineItem.ItemQty"))
//      .withColumn("TotalValue", expr("LineItem.TotalValue"))
//      .drop("LineItem")
//
//    df4.write.mode(SaveMode.Overwrite).parquet("output/")
//    logger.info("Finish writing data.")

//    val parqDF = spark.read.parquet("C:\\GitProject\\SparkProgrammingInScala\\22-ComplexTypesDemo\\src\\main\\scala\\guru\\learningjournal\\spark\\examples\\part-00000-deb93f20-b2d5-46b1-9183-8487b42ebfa5-c000.snappy.parquet")
//    parqDF.printSchema()
//    println("=================")
//    parqDF.limit(5).show(false)
//    println("=================")
//
//    val schema = parqDF.schema
//    val fields = schema.fields
////    fields.foreach()
//    println(fields.mkString("Array(", ", ", ")"))

    val fmt = "yyyy-MM-dd HH:mm:ss"
    val df = Seq(
      "2019-10-21 14:45:23",
      "2019-10-22 14:45:23",
      null,
      "2019-10-41 14:45:23", //invalid day
    )


//    df.wi("ts", to_timestamp($"ts", fmt))
//      .withColumn("ts", when($"ts".isNull, date_format(current_timestamp(), fmt)).otherwise($"ts"))
//      .show(false)

//    parqDF.select(col("Id"),col("Name"),to_json(col("ParentId")) as "ParentId",col("Type"),to_json(col("ParentId_ancestors")) as "ParentId_ancestors",to_json(col("children")) as "children").write.option("delimiter","|").csv("C:\\GitProject\\SparkProgrammingInScala\\22-ComplexTypesDemo\\src\\main\\scala\\guru\\learningjournal\\spark\\examples\\parquet_to_csv.csv")



  }

}
