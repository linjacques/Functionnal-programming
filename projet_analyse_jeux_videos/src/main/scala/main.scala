package example
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object Hello {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val pgUrl = config.getString("postgres.url")
    val pgUser = config.getString("postgres.user")
    val pgPwd = config.getString("postgres.password")
    val pgTable = config.getString("postgres.table")

    val spark = SparkSession.builder()
      .appName("Spark JDBC")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1") 
      .getOrCreate()

    val df = spark.read
      .format("jdbc")
      .option("url", pgUrl)
      .option("dbtable", pgTable)
      .option("user", pgUser)
      .option("password", pgPwd)
      .option("driver", "org.postgresql.Driver")
      .load()

      df.show(10)


    spark.stop()
  }
}
