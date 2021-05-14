package com.ssx.spark.demo

import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.ssx.spark.{AbstractApplication, JobConsts}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}

import scala.collection.mutable

/**
 * mongodb使用使用Demo
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.demo.TestMongoDb,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @author sunshuxian
 * @version 1.0
 * @create 2021/05/10 10:53
 **/

class TestMongoDb extends AbstractApplication {

  val mongoUrl = "mongodb://localhost:8080/aa.bb"

  override def setConf(conf: SparkConf, args: mutable.Map[String, String]): Unit = {
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {

    val targetDay = args.get(JobConsts.RUN_DAY).get

    val schema =new StructType()
      .add("_class",StringType)
      .add("_id",LongType)
      .add("array_list",ArrayType(new StructType().add("_id",NullType).add("test1",LongType).add("test2",TimestampType)))
      .add("username",StringType)
      .add("log_date",StringType)

    val df = sparkSession.read.schema(schema)
      .format("com.mongodb.spark.sql")
      .options(Map("spark.mongodb.input.uri" -> mongoUrl,
        // 根据大小进行分片
        "spark.mongodb.input.partitioner" -> "MongoPaginateBySizePartitioner",
        "spark.mongodb.input.partitionerOptions.partitionKey" -> "_id",
        "spark.mongodb.input.partitionerOptions.partitionSizeMB" -> "128"))
      .load()

    df.createOrReplaceTempView("t1")

    val sql = s"select array_list,username,log_date from t1 where log_date = '$targetDay'"

    val df2 = sparkSession.sql(sql)
    val df3 = df2.rdd.repartition(1).map(x => {
      val examIdsArray = new JSONArray()
      if (x.get(0) != null) {
        val list = x.getAs[Seq[Row]](0)
        for (x <- list) {
          val jsonObject = new JSONObject()
          jsonObject.put("examId", x.get(1))
          jsonObject.put("updateDate",x.get(2))
          examIdsArray.add(jsonObject)
        }
      }
      // TODO
      Row(examIdsArray.toString,x.getString(1),x.getString(2))
    })
    // TODO
  }
}


