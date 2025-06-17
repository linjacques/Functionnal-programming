package pipelines

import org.apache.spark.sql.SparkSession
import pipelines.extract.{ExtractFromCSV, ExtractFromPostgres}
import pipelines.transform.silver
import pipelines.load.{load__SalesByPlatform, load__GenrePerformance}
import datamart.{SalesByPlatform, GenrePerformance}

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("analyse jeux-vidéos")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    // ==============================
    //  EXTRACT (depuis CSV et PostgreSQL)
    // ==============================
    ExtractFromCSV.run(spark)
    ExtractFromPostgres.run(spark)

    // ==============================
    //  TRANSFORM (layer silver : nettoyage, standardisation)
    // ==============================
    silver.silver_layer(spark)

    // ==============================
    //  DATAMART (calculs analytiques, agrégations)
    // ==============================
    SalesByPlatform.run(spark)
    GenrePerformance.run(spark)

    // ==============================
    //  LOAD (chargement des marts dans PostgreSQL)
    // ==============================
    load__SalesByPlatform.run(spark)
    load__GenrePerformance.run(spark)

    spark.stop()
  }
}
