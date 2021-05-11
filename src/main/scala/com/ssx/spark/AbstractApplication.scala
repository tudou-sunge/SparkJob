package com.ssx.spark

import java.sql.{Connection, Timestamp}
import java.util.Properties

import com.ssx.spark.common.Source
import com.ssx.spark.common.DataExtract
import com.ssx.spark.utils.MySqlProperty
import org.apache.phoenix.jdbc.PhoenixDriver
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

trait AbstractApplication extends Logging with Serializable {


  def setConf(conf: SparkConf)

  def execute(sparkSession: SparkSession, args: mutable.Map[String, String])

  /**
   * 初始化任务并执行
   *
   * @param args 入参
   */
  def execute(args: mutable.Map[String, String]): Unit = {
    var sparkSession: SparkSession = null
    val sparkConf: SparkConf = initSparkConf(args.get(JobConsts.ARGS_CLASS_NAME).getOrElse("DefalultAppName"))
    val master = args.getOrElse("_master", "yarn")
    if ("local".equalsIgnoreCase(master)) sparkConf.setMaster("local[*]")
    // 支持个性化conf设置
    setConf(sparkConf)
    sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
    // 执行具体任务
    execute(sparkSession, args)
  }

  /**
   * 初始化spark conf
   *
   * @param appName 应用名称
   * @return
   */
  def initSparkConf(appName: String): SparkConf = {
    new SparkConf().setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 压缩
      .set("hive.exec.compress.output", "true")
      .set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec")
      // 动态分区
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      // 开启小文件合并
      .set("spark.sql.adaptive.enabled", "true")
      //在 map only 的任务结束时合并小文件
      .set("hive.merge.mapfiles", "true")
      // true 时在 MapReduce 的任务结束时合并小文件
      .set("hive.merge.mapfiles", "true")
      //合并文件的大小
      .set("hive.merge.mapredfiles", "true")
      //每个 Map 最大分割大小
      .set("hive.merge.size.per.task", "256000000")
      // 平均文件大小，是决定是否执行合并操作的阈值，默认16000000
      .set("mapred.max.split.size", "256000000")
      // 执行 Map 前进行小文件合并
      .set("hive.merge.smallfiles.avgsize", "256000000")
      // 大小写不敏感
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.caseSensitive", "false")

  }

  /**
   * 获取数据源
   */
  def getSource(sparkSession: SparkSession, sourceType: String, sourceId: Long) = {
    val result = sparkSession.read
      .format("jdbc")
      .option("url", MySqlProperty.URL)
      .option("driver", MySqlProperty.DRIVER)
      .option("user", MySqlProperty.USER)
      .option("password", MySqlProperty.PASSWORD)
      .option("dbtable", "source")
      .load
      .where("status = 1")
      .where(s"source_id = '$sourceId'")
      .collect().map { row => {
      val tmp = Source()
      tmp.sourceId = row.getAs[Long]("source_id")
      tmp.sourceName = row.getAs[String]("source_name")
      tmp.sourceComment = row.getAs[String]("source_comment")
      tmp.sourceType = row.getAs[String]("source_type")
      tmp.url = row.getAs[String]("url")
      tmp.dbName = row.getAs[String]("db_name")
      tmp.driver = row.getAs[String]("driver")
      tmp.userName = row.getAs[String]("user_name")
      tmp.password = row.getAs[String]("password")
      tmp.status = row.getAs[Int]("status")
      tmp.createBy = row.getAs[String]("create_by")
      tmp.createTime = row.getAs[Timestamp]("create_time")
      tmp.updateBy = row.getAs[String]("update_by")
      tmp.updateTime = row.getAs[Timestamp]("update_time")
      tmp
    }
    }

    result(0)
  }

  /**
   * 获取任务
   */
  def getJob(sparkSession: SparkSession, jobId: Long) = {
    val result = sparkSession.read
      .format("jdbc")
      .option("url", MySqlProperty.URL)
      .option("driver", MySqlProperty.DRIVER)
      .option("user", MySqlProperty.USER)
      .option("password", MySqlProperty.PASSWORD)
      .option("dbtable", "job")
      .load
      .where("job_status = 0")
      .where(s"job_id = $jobId")
      .collect()
      .map { row => {
        val tmp = DataExtract()
        tmp.jobId = row.getAs[Long]("job_id")
        tmp.jobName = row.getAs[String]("job_name")
        tmp.jobType = row.getAs[String]("job_type")
        tmp.jobComment = row.getAs[String]("job_comment")
        tmp.jobParam = row.getAs[String]("job_param")
        tmp.jobCycle = row.getAs[String]("job_cycle")
        tmp.jobExecuteTime = row.getAs[String]("job_execute_time")
        tmp.jobSonType = row.getAs[Int]("job_son_type")
        tmp.jobContent = row.getAs[String]("job_content")
        tmp.createBy = row.getAs[String]("create_by")
        tmp.createTime = row.getAs[Timestamp]("create_time")
        tmp.updateBy = row.getAs[String]("update_by")
        tmp.updateTime = row.getAs[Timestamp]("update_time")
        tmp.isRetry = row.getAs[Int]("is_retry")
        tmp.jobOwner = row.getAs[String]("job_owner")
        tmp.jobAlias = row.getAs[String]("job_alias")
        tmp.jobStatus = row.getAs[Int]("job_status")
        tmp.jobWeight = row.getAs[Int]("job_weight")
        tmp
      }
      }

    result(0)
  }

  /**
   * 获取Phoenix链接
   */

  def getPhoenixConnect(): Connection={
    val driver = new PhoenixDriver()
    val properties=new Properties()
    val connect=driver.connect("jdbc:phoenix:hdn1.dabig.com:2181",properties)
    connect.setAutoCommit(false)
    connect.setReadOnly(false)
    connect
  }

}
