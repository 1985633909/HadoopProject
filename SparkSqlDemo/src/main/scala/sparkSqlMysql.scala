
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


/**
 * @author 19856
 * @since 2023/4/11-22:59
 */
case class Person(name:String,age:Long)
object sparkSqlMysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sparkSqlMysql").master("local").getOrCreate()
    val sc = spark.sparkContext
    //读取数据
    val data:RDD[Array[String]] = sc.textFile("D:/tmp/people.txt").map(x=>x.split(","))
    //RDD关联Person
    val personRdd:RDD[Person]=data.map(x=>Person(x(0),x(1).toLong))
    //导入隐式转换
    import  spark.implicits._
    //将RDD转换成DataFrame
    val peronDF :DataFrame = personRdd.toDF()
    peronDF.show()
    //创建Properties对象连接数据库
    val prop=new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","changjiacheng1")

    peronDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.133.100:3306/spark","person",prop)
    //从数据库读取数据
    val mysqlDF:DataFrame = spark.read.jdbc("jdbc:mysql://192.168.133.100:3306/spark","person",prop)
    mysqlDF.show()
    spark.stop()
  }
}
