import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @author 19856
 * @since 2023/4/12-23:30
 */
object KafkaConsumerMsg {
/*Spark Streaming 整合 Kafka Receiver 方法*/
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaConsumerMsg")
    //创建SparkContext对象
    val sparkContext = new SparkContext(sparkConf)
    //设置日志级别
    sparkContext.setLogLevel("warn")
    //获取StreamingContext对象
    val ssc = new StreamingContext(sparkContext,Seconds(5))
    //设置Kafka参数
    val kafkaParams = Map("bootstrap.servers"->"hadoop100:9092,hadoop101:9092,hadoop102:9092","group.id"->"spark-receiver")
    //指定Topic相关信息
    val topics = Set("logtoflume")
    //通过KafkaUtils.create利用低级API接受Kafka数据
    val kafkaStream:InputDStream[(String,String)]=
      KafkaUtils.createDirectStream
        [String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
    //获取kafka中topic数据，并解析json格式
    val events:DStream[JSONObject] = kafkaStream.flatMap(line=>Some(JSON.parseObject(line._2)))
    //按照productId进行分组统计个数和总价格
    val orders:DStream[(String,Int,Long)] = events.map(x=>(x.getString("productId"),x.getLong("productPrice"))).groupByKey()
      .map(x=>(x._1,x._2.size,x._2.reduceLeft(_ + _)))
    //打印输出
    orders.foreachRDD(x=>
    x.foreachPartition(partition=>
      partition.foreach(x=>{
        println("productId="+x._1+"count="+x._2+"productPrice="+x._3)
      })
    ))
    ssc.start()
    ssc.awaitTermination()


  }

}

