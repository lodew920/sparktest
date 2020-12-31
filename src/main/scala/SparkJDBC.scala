import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJDBC {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("Spark-JDBC")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    val frame: DataFrame = sparkSession.read.format("jdbc").option("url", "jdbc:mysql://node-1:3306/spark")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "abc")
      .load()


  }
  def updateToHive(): Unit ={


  }

}
