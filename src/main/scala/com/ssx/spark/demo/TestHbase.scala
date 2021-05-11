package com.ssx.spark.demo

import com.ssx.spark.AbstractApplication
import org.apache.phoenix.spark.{DataFrameFunctions, PhoenixRDD}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Hbase使用Demo
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.demo.TestHbase,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @author sunshuxian
 * @version 1.0
 * @create 2021/05/10 10:53
 **/
class TestHbase extends AbstractApplication {

  case class Person(MY_PK: Long, FIRST_NAME: String,LAST_NAME:String)

  override def setConf(conf: SparkConf): Unit = {}

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {

    //读取hbase数据
    val sc = sparkSession.sparkContext
    val table = "test.example"
    val columns = Seq("MY_PK", "FIRST_NAME", "LAST_NAME")
    val zkUrl = Some("hdn1.dabig.com")
    val conf = sc.hadoopConfiguration

    log.info(".....................................................")
    //获取PhoenixRDD
    val frdd = new PhoenixRDD(sc = sc, table = table, columns = columns, zkUrl = zkUrl, conf = conf)
    //转化成DataFrame
    val a = frdd.toDataFrame(sparkSession.sqlContext)
    a.show()

    log.info(".....................................................")
    //方式2：
    val b = sparkSession.read
      .format("org.apache.phoenix.spark")
      .option("table", table)
      .option("zkUrl", "hdn1.dabig.com")
      .load()
    b.show(10)

    //方式3：
    val connect = getPhoenixConnect()
    val stmt = connect.createStatement()
    //查询
    val c = stmt.executeQuery("select * from test.example limit 100")
    while (c.next()) {
      log.info(c.getLong("MY_PK").toString)
    }

    // 单条更新hbase
//    stmt.executeUpdate(sql)
//    connect.commit()
//    connect.close()

    // 批量更新hbase //放回每条记录更新影响的行数
//    for (sql <- batchSQL) {
//      stmt.addBatch(sql)
//    }
//    stmt.executeBatch
//    connect.commit()
//    connect.close()


    //写入Hbase
    //创建DataFrame
    val data = Seq(Person(22, "zhang", "aaaaaaa"), Person(33, "li", "bbbbbb"), Person(34, "acd", "sdf"))
    val writeDF = sparkSession.createDataFrame(data)
    //DataFrame转化成DataFrameFunctions
    val phoenixRecordWriter: DataFrameFunctions = new DataFrameFunctions(writeDF)
    //调用saveToPhoenix方法,主键存在的情况下会做Update，主键不存在的情况下会Insert
    phoenixRecordWriter.saveToPhoenix(tableName = table, conf = conf, zkUrl = zkUrl)


  }
}
