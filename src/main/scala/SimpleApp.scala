/* SimpleApp.scala */

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val wordFile = "hdfs://node-1:9000/user/Hadoop/word.txt"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(wordFile)
    val wordcount = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    wordcount.saveAsTextFile("word2.txt")
    sc.stop()

  }
}
