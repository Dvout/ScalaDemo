
import java.util.{Date, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


case class Score(userId:Int, movieId:Int, score:Float,comment_time:Int)

object ImportRating {
  def main(args: Array[String]) {
    var start_time =new Date().getTime


    //todo:1、创建sparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSqlToMysql")
      .getOrCreate()
    //todo:2、读取数据
    val data: RDD[String] = spark.sparkContext.textFile("hdfs://localhost:9000/movieData/ratings.dat")
    //todo:3、切分每一行,
    val arrRDD: RDD[Array[String]] = data.map(_.split("::"))
    //todo:4、RDD关联Score
    val scoreRDD: RDD[Score] = arrRDD.map(x=>Score(x(0).toInt,x(1).toInt,x(2).toFloat,x(3).toInt))
    //todo:导入隐式转换
    import spark.implicits._
    //todo:5、将RDD转换成DataFrame
    val scoreDF: DataFrame = scoreRDD.toDF()
    //todo:6、将DataFrame注册成表
    scoreDF.createOrReplaceTempView("score")
    //todo:7、操作Score表 ,按照userId进行降序排列
    val resultDF: DataFrame = spark.sql("select * from score order by userId")

    //todo:8、把结果保存在mysql表中
    //todo:创建Properties对象，配置连接mysql的用户名和密码
    val prop =new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")

//    resultDF.write.jdbc("jdbc:mysql://localhost:3306/sparkMovie?useSSL=false","movieScore",prop)

    //todo:写入mysql时，可以配置插入mode，overwrite覆盖，append追加，ignore忽略，error默认表存在报错
    resultDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/sparkMovie?useSSL=false","score",prop)
    spark.stop()

    var end_time =new Date().getTime
    println((end_time-start_time)/(1000)+"s") //单位毫秒
  }
}
