import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 19856
 * @since 2023/4/12-19:29
 */
object coGroupAndJoin {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf().setAppName("cogropAndJoin").setMaster("local")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //创建两个可被并行操作的分布式数据集
    val idName = sc.parallelize(Array((1,"张三"),(2,"李四"),(3,"王五")))
    val idAge = sc.parallelize(Array((1,30),(2,29),(3,18)))
    println("\ncogroup\n")
    //对两个并行数据集进行cogroup操作
    idName.cogroup(idAge).collect().foreach(println)
    println("\njoin\n")
    //对两个并行数据集进行join操作
    idName.join(idAge).collect().foreach(println)


  }

}
