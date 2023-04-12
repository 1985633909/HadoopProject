import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 19856
 * @since 2023/4/12-20:33
 */
object SparkStreamingSocketWindow {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val sparkConf:SparkConf=new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSocketWindow")
    //创建SparkContext对象
    val sparkContext = new SparkContext(sparkConf)
    //设置日志级别
    sparkContext.setLogLevel("warn")
    //获取StreamContext对象，每五秒一次
    val ssc = new StreamingContext(sparkContext,Seconds(5))
    //接受Socket的数据
    val textStream:ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    //获取每一行的单词
    val words:DStream[String]=textStream.flatMap(_.split(" "))
    //将每个单词置为1
    val wordAndOne:DStream[(String,Int)] = words.map((_,1))
    //每隔十秒统计最近10s的搜索词出现的次数
    val result:DStream[(String,Int)]=wordAndOne.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(10),Seconds(10))
    //打印
    result.print()
    //开启流式处理
    ssc.start()
    ssc.awaitTermination()
  }
}
