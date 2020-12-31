import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object SparkSql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir","spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    val df: DataFrame = spark.read.json("./people.json")
    df.show()

    //JDBC数据库读写
    val jdbc: DataFrame = spark.read.format("jdbc").option("url","jdbc:mysql://node-1:3306/spark")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","student")
      .option("user","root")
      .option("password","abc")
      .load()
    jdbc.show(10)

    val studentRDD: RDD[Array[String]] = spark.sparkContext.parallelize(Array("5 李阳 M 88","6 gg M 18")).map(_.split( " "))
    val schema: StructType = StructType(
      List(
        StructField("id", IntegerType,nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("gender", StringType, nullable = true),
        StructField("age",IntegerType, nullable = true)
      )
    )
    val rowRDD: RDD[Row] = studentRDD.map(p=> Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))
    val studentDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","abc")
    prop.put("driver","com.mysql.jdbc.Driver")

    studentDF.write.mode("append").jdbc("jdbc:mysql://node-1:3306/spark","spark.student",prop)
    val dataFrame: DataFrame = spark.read.jdbc("jdbc:mysql://node-1:3306/spark", "spark.student", prop)
    dataFrame.show(10)

//    Spark连接Hive
    spark.sql("select * from spark.student").show(10)
    studentDF.createOrReplaceTempView("tempTable")
    spark.sql("insert into sparktest.student select * from tempTable")







  }



}
