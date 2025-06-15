package pipelines.transform
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.functions.{col, lower, trim, substring}

object silver {
  def silver_layer(spark: SparkSession): Unit = {

    val df_sales = spark.read.parquet("../lakehouse/bronze/vgsales")
    val df_info = spark.read.parquet("../lakehouse/bronze/video_games")


    // Comptage des lignes
    println(s"Nombre de lignes dans df_info : ${df_info.count()}")
    println(s"Nombre de lignes dans df_sales : ${df_sales.count()}")

    // Extraire les 4 derniers caractères de release_date pour avoir l’année
    val df_info_year = df_info.withColumn("release_year", substring(col("release_date"), -4, 4))


    // Normalisation des noms (minuscules + trim)
    val df_sales_clean = df_sales.withColumn("Name", lower(trim(col("Name"))))
    val df_info_clean = df_info_year.withColumn("name", lower(trim(col("name"))))

    //Renommage des colonnes
    val df_sales_fin = df_sales_clean.withColumnRenamed("Name", "sales_Name")
    val df_info_fin = df_info_clean.withColumnRenamed("Name", "info_Name")
    val df_sales_fini = df_sales_fin.withColumnRenamed("Platform", "sales_platform")
    val df_info_fini = df_info_fin.withColumnRenamed("platform", "info_platform")


    // 6. Jointure sur "Name" et l’année
    // Jointure
    val df_joint = df_sales_fini.join(
      df_info_fini,
      col("sales_Name") === col("info_Name") && col("Year") === col("release_year"),
      "inner"
    )

    // Affichage des colonnes sélectionnées
    val df_joint_fini = df_joint.select(
      "Rank", "sales_Name", "sales_platform", "Year",
      "Genre", "Publisher",
      "NA_Sales", "EU_Sales", "JP_Sales", "Other_Sales", "Global_Sales",
      "user_review", "summary"
    )

    // Suppression des lignes contenant des null
    val df_fini = df_joint_fini.na.drop()

    // Compte lignes et colonnes après nettoyage
    println(s"Nombre de lignes après suppression des NA : ${df_fini.count()}")
    println(s"Nombre de colonnes après suppression des NA : ${df_fini.columns.length}")

    val nb_uniques_2 = df_fini.select("sales_Name").distinct().count()
    println(s"Nombre de jeux différents (sales_Name) après nettoyage : $nb_uniques_2")

    df_fini.write.mode("overwrite").parquet("../lakehouse/silver")

  }
}
