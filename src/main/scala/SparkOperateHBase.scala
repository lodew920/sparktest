



import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.exceptions
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos

//import org.apache.hadoop.hbase.protobuf.ProtobufUtil
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
//import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException

object SparkOperateHBase {


  def main(args:Array[String]): Unit ={
//    只取前100数据
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("content"))
    scan.withStartRow(Bytes.toBytes("1"), true)
    scan.withStopRow(Bytes.toBytes("429"), true)

    val conf: Configuration = HBaseConfiguration.create()
    val conf1 = new SparkConf().setMaster("local[*]").setAppName("simple test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf1)

    val companyRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result]);

    //1. 统计全部的个数
    val count: Long = companyRDD.count()
    println("company count:"+count)
    companyRDD.cache()


    //2. 因为数据太大，所以只遍历输出前十个
    companyRDD.take(10).foreach({ case (_,result) =>
      val stockId: String = Bytes.toString(result.getRow)
      val stockNickname: String = Bytes.toString(result.getValue("content".getBytes,"stockNickname".getBytes))
      val name: String = Bytes.toString(result.getValue("content".getBytes,"name".getBytes))
      val province: String = Bytes.toString(result.getValue("content".getBytes,"province".getBytes))
      val city: String = Bytes.toString(result.getValue("content".getBytes,"city".getBytes))
      val income: String = Bytes.toString(result.getValue("content".getBytes,"income".getBytes))
      val profit: String = Bytes.toString(result.getValue("content".getBytes,"profit".getBytes))
      val peopleNum: String = Bytes.toString(result.getValue("content".getBytes,"peopleNum".getBytes))
      val listDate: String = Bytes.toString(result.getValue("content".getBytes,"listDate".getBytes))
      val category: String = Bytes.toString(result.getValue("content".getBytes,"category ".getBytes))
      val business: String = Bytes.toString(result.getValue("content".getBytes,"business".getBytes))
      println("Row key:"+stockId+" stockNickName:"+stockNickname+" name:"+name+" province:"+province+" city:"+city+" income:"+income+" profit:"+profit+" peopleNum:"+peopleNum+
        " listDate:"+listDate+" category:"+category+" business:"+business)

    })


    //3. 统计一共有多少行业，以及每个行业的数量
    println("统计一共有多少行业，以及每个行业的数量")
    val rddlist: RDD[String] = companyRDD.map((rdd: (ImmutableBytesWritable, Result)) => Bytes.toString(rdd._2.getValue("content".getBytes, "category".getBytes)))
    val rddmaplist: RDD[(String, Int)] = rddlist.map((_, 1))
    val rddmaplist2: RDD[(String, Int)] = rddmaplist.reduceByKey((a, b) => a + b)
    val rddmaplist3: RDD[(String, Int)] = rddmaplist2.sortBy(f => (f._2, f._1), ascending = false)
    rddmaplist3.foreach(println)



    //4. 统计中国各个城市有多少家公司
    println("统计中国各个城市有多少家公司")
    companyRDD.map((rdd: (ImmutableBytesWritable, Result)) => Bytes.toString(rdd._2.getValue("content".getBytes, "city".getBytes)))
      .map((_, 1)).reduceByKey((a, b) => a + b).sortBy(f => (f._2, f._1), ascending = false).foreach(println)


    //5. 统计收入最高的前5个公司
    companyRDD.map((rdd: (ImmutableBytesWritable, Result)) => Bytes.toString(rdd._2.getValue("content".getBytes, "name".getBytes)))
      .map((_, 1)).reduceByKey((a, b) => a + b).sortBy(f => (f._2, f._1), ascending = false).take(5).foreach(println)


  }


}
