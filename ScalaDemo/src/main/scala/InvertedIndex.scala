import org.apache.spark.sql.SparkSession

/**
 * @author 19856
 * @since 2023/4/11-19:51
 */
object InvertedIndex {
  def main(args: Array[String]) {
    //获取sparkSession对象
    val spark = SparkSession.builder().appName("InvertedIndex").master("local").getOrCreate()
    //读取目录test
    val data = spark.sparkContext.wholeTextFiles("D:/tmp/test")
    //使用分割"/''获取文件名
    val r1 = data.flatMap { x =>
      val doc = x._1.split(s"/").last.split("\\.").head
      //先按行切分,在按列空格进行切分
      x._2.split("\r\n").flatMap(_.split(" ").map { y => (y, doc) })
    }
      .groupByKey.map { case (x, y) => (x, y.toSet.mkString(",")) }
    //.groupByKey().map(y=>(y._1,y._2.toList.mkString(","))
    r1.foreach(println)
  }
}
