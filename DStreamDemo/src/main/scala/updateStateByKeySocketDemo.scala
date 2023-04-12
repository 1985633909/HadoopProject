import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 19856
 * @since 2023/4/12-20:50
 */
object updateStateByKeySocketDemo {
  def main(args: Array[String]): Unit = {
    //定义状态更新函数
    val updateFunc = (currValues:Seq[Int],prevValueState:Option[Int])=>{
      //返回结果对象初始值为零，累计currentCount集合中的每个值
      val currentCount = currValues.foldLeft(0)(_ +_ )
      //已累加的值
      val previousCount = prevValueState.getOrElse(0)
      //返回累加后的结果
      Some(currentCount+previousCount)
    }
    //创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("updateStateByKeySocketDemo").setMaster("local[2]")
    //创建SparkContext对象
    val sparkContext = new SparkContext(sparkConf)
    //获取StreamingContext对象，5s一个批次
    val ssc=new StreamingContext(sparkContext,Seconds(5))
    //设置日志级别
    sparkContext.setLogLevel("warn")
    //开启检查点
    ssc.checkpoint("E:/checkpoint")
    //接受数据源，产生DStream对象
    val lines = ssc.socketTextStream("localhost",9999)
    //以空格分割单词
    val words = lines.flatMap(_.split(" "))
    //将分割后的每个单词出现次数记录为1
    val wordDstream = words.map(x=>(x,1))
    //通过键的先前值和新值上应用给定函数updateFunc更新每一个键的状态
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    //输出打印
    stateDstream.print()
    //新加的代码
    stateDstream.saveAsTextFiles("E:/log/output.txt")
    //开启实时计算
    ssc.start()
    //等待应用停止
    ssc.awaitTermination()

  }
}
