import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.nio.file.Files.lines


/**
 * @author 19856
 * @since 2023/4/12-18:54
 */
object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf, Seconds(10)) //每隔10秒统计一次字符总数
    //创建珍一个DStream，连接master:9998
    val lines = sc.textFileStream("E:/log")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    sc.start() //开始计算
    sc.awaitTermination() //通过手动终止计算，否则一直运行下去
  }
}
