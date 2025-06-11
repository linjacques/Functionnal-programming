package pipelines.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.Paths

object ExtractFromCSV {

  def readCSV(spark: SparkSession): DataFrame = {
    val path = Paths.get("src/main/resources/vgsales.csv").toAbsolutePath.toString

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def writeParquet(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").parquet(outputPath)
  }

  def run(spark: SparkSession): Unit = {
    val df = readCSV(spark)
    writeParquet(df, "../lakehouse/bronze/vgsales/")
  }
}
