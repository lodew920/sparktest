
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

object QueueStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TestDStream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
//    val lines: DStream[String] = ssc.textFileStream("file:///opt/module/spark-3.0.1/mycode/streaming/logfile")
//    val words: DStream[String] = lines.flatMap((a: String) => a.split(" "))
//    val wordcounts: DStream[(String, Int)] = words.map((x: String) =>(x,1)).reduceByKey((a: Int, b: Int)=>a+b)
//    wordcounts.print()
    val rddQueue =new scala.collection.mutable.SynchronizedQueue[RDD[Int]]()
    ssc.queueStream(rddQueue).map((r: Int) =>(r%10,1)).reduceByKey((_: Int)+(_: Int)).print()
    ssc.start()
    for(i<-1 to 10){
      rddQueue+=ssc.sparkContext.makeRDD(1 to 100, 2)
      Thread.sleep(1000)
    }
    ssc.stop()
//    ssc.sparkContext.makeRDD(1 to 100, 2).foreach(println)


  }

}
