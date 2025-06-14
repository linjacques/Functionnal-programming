package pipelines

import org.apache.spark.sql.SparkSession
import pipelines.extract.ExtractFromCSV
import pipelines.extract.ExtractFromPostgres
import pipelines.transform.silver

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("VideoGameETL")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    ExtractFromCSV.run(spark)
    ExtractFromPostgres.run(spark)
    silver.silver_layer(spark)

    spark.stop()
  }
}
