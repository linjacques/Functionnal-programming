package datamart

import java.nio.file.Paths
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SalesByPlatform {

  def readParquet(spark: SparkSession): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .parquet("../lakehouse/silver")
  }

  def transform(df: DataFrame): DataFrame = {
    df.groupBy("sales_platform")
      .agg(sum("global_sales").as("total_sales"))
      .orderBy(desc("total_sales"))
  }

  def write(result: DataFrame): Unit = {
    result.write.mode("overwrite").parquet("../lakehouse/gold/SalesByPlatform/")
  }

  def run(spark: SparkSession): Unit = {
    val df = readParquet(spark)
    val result = transform(df)
    write(result)
  }
}