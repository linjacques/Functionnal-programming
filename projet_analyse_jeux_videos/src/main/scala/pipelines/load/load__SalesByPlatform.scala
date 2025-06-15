package pipelines.load
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import com.typesafe.config.ConfigFactory

object load__SalesByPlatform {

  def readParquet(spark: SparkSession, parquetPath: String): DataFrame = {
    spark.read.parquet(parquetPath)
  }
 
  def writeToPostgres(df: DataFrame, table: String): Unit = {
    val config = ConfigFactory.load()
    val url = config.getString("postgres.url")
    val user = config.getString("postgres.user")
    val password = config.getString("postgres.password")
    val props = new Properties()
    
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", "org.postgresql.Driver")

    df.write
        .mode("overwrite")
        .jdbc(url, table, props)
  }
   
  def run(spark: SparkSession): Unit = {
    val df = readParquet(spark, "../lakehouse/gold/SalesByPlatform")
    writeToPostgres(df, "SalesByPlatform")
  }
}
