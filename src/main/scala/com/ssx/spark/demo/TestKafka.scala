package com.ssx.spark.demo

import java.util
import java.util.Properties

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.ssx.spark.{AbstractApplication, JobConsts}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable

/**
 * Kafka使用使用Demo
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.demo.TestKafka,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @author sunshuxian
 * @version 1.0
 * @create 2021/05/10 10:53
 **/
class TestKafka extends AbstractApplication {

  override def setConf(conf: SparkConf): Unit = {

  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]) = {

    val yesterday: String = args.get(JobConsts.RUN_DAY).get
    val KAFKA_SERVERS: String = "kafn1.dabig.com:9092,kafn2.dabig.com:9092,kafn3.dabig.com:9092"
    val KAFKA_TOPIC: String = "test"

    val localDt: DateTime = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHours(8))
      .parseDateTime(yesterday)

    val utcDt = localDt.toDateTime(DateTimeZone.UTC)
    val yesterdayUTC = utcDt.toString()

    var sql = "select * from xxx limit 1000"
    log.info(sql)
    val studyDF = sparkSession.sql(sql)

    studyDF.map(row => {
      val result = new JSONObject
      result.put("user_id", row.getLong(0))
      result.put("mark", "Info")
      result.put("update_date", yesterdayUTC)
      val studyInfoMap = new JSONObject
      studyInfoMap.put("1", row.getString(1))
      studyInfoMap.put("2", row.getString(2))
      studyInfoMap.put("3", row.getString(3))
      result.put("studyInfo", studyInfoMap)
      result.toJSONString
    })(Encoders.STRING)
      .foreachPartition(row => {
        val props = new Properties
        props.put("bootstrap.servers", KAFKA_SERVERS)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        while (row.hasNext) { // 发送消息
          producer.send(new ProducerRecord[String, String](KAFKA_TOPIC, row.next))
          producer.flush()
        }
        producer.close()
      })

  }

}
