package datamart

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.nio.file.Paths

object ArgentRaw {
    def readParquet(spark: SparkSession): DataFrame = {
        spark.read.option("inferSchema", "true").parquet("../lakehouse/silver")
    }

    def write(df: DataFrame): Unit = {
        df.write.mode("overwrite").parquet("../lakehouse/gold/silver/")
    }

    def run(spark: SparkSession): Unit = {
        val df = readParquet(spark)
        write(df)
    }
}