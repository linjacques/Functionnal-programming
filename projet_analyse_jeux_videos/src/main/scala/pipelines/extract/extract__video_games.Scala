package pipelines.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths

object ExtractFromPostgres {

  def readFromPostgres(spark: SparkSession): DataFrame = {
    val config = ConfigFactory.load()
    val url = config.getString("postgres.url")
    val user = config.getString("postgres.user")
    val password = config.getString("postgres.password")
    val table = config.getString("postgres.table")

    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def writeParquet(df: DataFrame, outputPath: String): Unit = {
    val absPath = Paths.get(outputPath).toAbsolutePath.toString
    df.write.mode("overwrite").parquet(absPath)
  }

  def run(spark: SparkSession): Unit = {
    val df = readFromPostgres(spark)
    val outputPath = "../lakehouse/bronze/video_games/"
    writeParquet(df, outputPath)
  }
}
