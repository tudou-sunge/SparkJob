package com.ssx.spark.demo

import com.mongodb.spark.config.{AggregationConfig, ReadConcernConfig, ReadConfig, ReadPreferenceConfig}
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.ssx.spark.AbstractApplication
import com.ssx.spark.utils.MongoPartitionUserDefined
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable

;

/**
 * mongodb使用使用Demo
 * 使用自定义分片规则进行分片
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "classNamecom.ssx.spark.demo.TestMongoDbPartition,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @author sunshuxian
 * @version 1.0
 * @create 2021/05/10 10:53
 **/
class TestMongoDbPartition extends AbstractApplication {
  val mongoUrl = "mongodb://syncdb01.dabig.me:27119/phoenix-course-listen.listenCourseRecord_"
  val tmpTable = "test.mongodb_course_listen_"

  override def setConf(conf: SparkConf): Unit = {
    //设置每个executor的内存
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {

    val mongoReadConfig = new ReadConfig(databaseName = "testdb",
      collectionName = "testcollectName",
      connectionString = None,
      sampleSize = 1000,
      partitioner = MongoPartitionUserDefined,
      //partitionerOptions中的配置将会被 MongoPartitionUserDefined方法partitions 中的readConfig参数接收
      partitionerOptions = Map(),
//      partitioner = MongoPaginateBySizePartitioner,
//      partitionerOptions = Map("spark.mongodb.input.partitionerOptions.partitionKey" -> "_id", "spark.mongodb.input.partitionerOptions.numberOfPartitions" -> "32"),
      localThreshold = 15,
      readPreferenceConfig = new ReadPreferenceConfig(),
      readConcernConfig = new ReadConcernConfig(),
      aggregationConfig = AggregationConfig(),
      registerSQLHelperFunctions = false,
      inferSchemaMapTypesEnabled = true,
      inferSchemaMapTypesMinimumKeys = 250,
      pipelineIncludeNullFilters = true,
      pipelineIncludeFiltersAndProjections = true,
      samplePoolSize = 1000,
      // 批量读取参数默认是空
      batchSize = Option(1000)
    )

    val mongoConnector = MongoConnector(Map("spark.mongodb.input.uri" -> "mongodb://syncdb01.dabig.me:27119", "spark.mongodb.input.localThreshold" -> "15"))

    val mongoSpark = MongoSpark.builder()
      .sparkSession(sparkSession)
      .readConfig(mongoReadConfig)
      .connector(mongoConnector)
      .build()
    mongoSpark.toDF().show(100, false)
    //mongoSpark.toDF().where("everyDate = '2021-01-01' ").show(100, false)

  }

}

