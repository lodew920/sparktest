import java.io.{BufferedReader, DataInputStream, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}

object test {
  def main(args:Array[String]): Unit ={

    val conf = new Configuration()
    conf.set("fs.defaultFS","hdfs://node-1:9000")
    val fs: FileSystem = FileSystem.get(conf)
    val path  = new Path("/user/Hadoop/sparktest")
    if(fs.exists(path)){
      println("源文件存在")
      fs.delete(path,true)
      println("原文件删除成功")
    }
    val output: FSDataOutputStream = fs.create(path)
    val writer = new PrintWriter(output)
    writer.write("this is a test from guo xue jian\nhow are you\nthis is for homework\nok thank you\n");
    writer.print("how are you\n")
    writer.println("lu er")
    writer.append("hello hello, let me know\n")
    writer.write("over over\n")
    writer.flush()
    writer.close();
    fs.close()
    println("写入数据结束了")
  }

}
