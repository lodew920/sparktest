
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object SparkHive {
  //1. 读取SQL数据，生成DataFrame
  //2. hive中建表
  //3. 将DataFrame中的数据储存到Hive数据
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("spark-hive")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()
    //读取Hive数据
    print("读取Hive中的前十条数据")
    sparkSession.sql("select * from sparktest.company limit 10,10").show()
    //向hive中添加数据
    print("读取完毕")
    val array = Array("18%神城A退%神州长城股份有限公司%深圳市%龙岗区%64.97亿%3.84亿%821%1992-06-16%装修装饰%建筑装饰工程设计与施工及建筑相关工程施工")
    val companyRDD: RDD[String] = sparkSession.sparkContext.parallelize(array)
    val companylistrdd: RDD[Array[String]] = companyRDD.map(_.split("%"))
    val rowRDD = companylistrdd.map(p=>Row(p(0).trim,p(1).trim,p(2).trim,p(3).trim,p(4).trim,
      p(5).trim,p(6).trim,p(7).trim,p(8).trim,p(9).trim,p(10).trim))
    val schema = StructType(
      List(
        StructField("stockid", StringType, true),
        StructField("sticknickname",StringType, true),
        StructField("name",StringType, true),
        StructField("province",StringType, true),
        StructField("city",StringType, true),
        StructField("income",StringType, true),
        StructField("profit",StringType, true),
        StructField("peoplenum",StringType, true),
        StructField("listdate",StringType, true),
        StructField("category",StringType, true),
        StructField("business",StringType, true)
      )
    )
    val frame: DataFrame = sparkSession.createDataFrame(rowRDD, schema)
    frame.createOrReplaceTempView("temptable")
    sparkSession.sql("insert into sparktest.company select * from temptable")
    print("插入完毕")


  }
}