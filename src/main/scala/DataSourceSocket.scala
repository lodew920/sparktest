import java.io.PrintWriter
import java.net.{ServerSocket, Socket}
import java.util.Random

import scala.io.{BufferedSource, Source}
// 读取一个文件，创建一个socket输入源，定时随机输出该文件的单词到socket端口;
object DataSourceSocket {
  def index(length:Int): Int ={
    val random = new Random()
    random.nextInt(length)

  }

  def main(args: Array[String]): Unit = {
    if(args.length!=3){
      System.err.println("Usage :<filename> <port> <millisecond>")
      System.exit(1)
    }
    val FileName: String = args(0)
    val bufferedSource: BufferedSource = Source.fromFile(FileName)
    val iterator: Iterator[String] = bufferedSource.getLines()
    val lines: List[String] = iterator.toList
    val rowCount: Int = lines.length
    val serverSocket = new ServerSocket(args(1).toInt)
    while(true){
      val socket: Socket = serverSocket.accept()
      new Thread(){
        override def run(): Unit = {
          println("Go client connected from:"+socket.getInetAddress)
          val writer = new PrintWriter(socket.getOutputStream, true)
          while(true){
            Thread.sleep(args(2).toLong)
            val content: String = lines(index(rowCount))
            println(content)
            writer.println(content)
            writer.flush()
          }
          socket.close()

        }
      }.start()

    }




  }

}
