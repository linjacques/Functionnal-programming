package datamart

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.Paths

object GenrePerformance {

  def readParquet(spark: SparkSession): DataFrame = {
    spark.read.option("inferSchema", "true").parquet("../lakehouse/silver")
  }

  def transform(df: DataFrame): DataFrame = {
    df.groupBy("Genre")
      .agg(
        count("*").as("nb_jeux"),
        sum("Global_Sales").as("total_sales")
      )
      .orderBy(desc("total_sales"))
  }

  def write(result: DataFrame): Unit = {
    result.write.mode("overwrite").parquet("../lakehouse/gold/GenrePerformance/")
  }

  def run(spark: SparkSession): Unit = {
    val df = readParquet(spark)
    val result = transform(df)
    write(result)
  }
}
