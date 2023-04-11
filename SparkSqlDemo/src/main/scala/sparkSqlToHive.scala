import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 19856
 * @since 2023/4/11-23:20
 */
object sparkSqlToHive {
  def main(args: Array[String]): Unit = {
    //设置访问用户名，主要用于访问HDFS下的Hive warahouse目录
    System.setProperty("HADOOP_USER_NAME","root")
    //创建sparkSession
     val spark:SparkSession = SparkSession.builder()
      .appName("sparksqlToHIVE").config("executor-cores",1).master("local").enableHiveSupport().getOrCreate();
    val sc = spark.sparkContext
    //读取文件
    val data:RDD[Array[String]] = sc.textFile("D:/tmp/people.txt").map(x=>x.split(","))
    //与RDD与样理类关联
    val personRdd:RDD[Person] = data.map(x=>Person(x(0),x(1).toLong))
    //导入隐式转换
    import spark.implicits._
    //将RDD转换成DataFrame
    val peronDF: DataFrame = personRdd.toDF()
    peronDF.show()
    //将DataFrame注册成临时表t_person
    peronDF.createOrReplaceTempView("t_person")
    //显示临时表t_person的数据
    spark.sql("select * from t_person").show()
    //使用hive中的bigdata数据库
    spark.sql("use bigdata")
    //将临时表的数据插入person表中
    spark.sql("insert into person select * from t_person")
    //显示用Hive中的bigdata数据库下的person表数据
    spark.sql("select * from person").show()
    spark.stop()













  }
}
