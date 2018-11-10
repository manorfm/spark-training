package com.thoughtworks.exercises.batch

import java.util.Properties

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NeatTotal {
  def main(args: Array[String]): Unit = {
    val log = LogManager.getLogger(this.getClass)

    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream(s"/application.properties"))
    val baseBucket = properties.getProperty("base_bucket")
    val username = properties.get("username")
    val dataFilesBucket = properties.getProperty("data_files_bucket")

    val ordersBucket = s"$baseBucket/$username/$dataFilesBucket/orders"
    val orderItemsBucket = s"$baseBucket/$username/$dataFilesBucket/orderItems"
    val productsBucket = s"$baseBucket/$username/$dataFilesBucket/products"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Engineering Capability Development - ETL Exercises")
      .getOrCreate()

    val orderItemsDF = spark.read
      .option("header", true)
      .option("infer_schema", true)
      .option("delimiter", ";")
      .csv(orderItemsBucket)
        //.createTempView("orderItems")

    val productsDF = spark.read
      .option("header", true)
      .option("infer_schema", true)
      .option("delimiter", ";")
      .csv(productsBucket)
        //.createTempView("products")

    import spark.implicits._

    val total = orderItemsDF.join(productsDF, Seq("ProductId"), "inner")
      .groupBy($"ProductId", $"Name")
      .agg(
        //sum($"Price" - $"Discount"),
        sum(($"Price" - $"Discount") * $"Quantity").as("Total"))
      .drop($"ProductId")
      .select(sum($"Total").as("Total"))
      //.show(false)
      .collect()(0).getAs[Double](0)
    //74.988,48

    print($"valor total: $total")

    //185.670.050.745
    //cento e oitenta e cinco bilhões, seiscentos e setenta milhões, cinquenta mil e setecentos e quarenta e cinco
  }
}
