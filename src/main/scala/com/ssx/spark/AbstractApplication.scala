package com.ssx.spark

import java.sql.{Connection, Timestamp}
import java.util.Properties

import cn.hutool.db.ds.simple.SimpleDataSource
import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.ssx.spark.common.{Job, Source}
import com.ssx.spark.job.PartitionBy
import com.ssx.spark.logs.LogRecord
import org.apache.phoenix.jdbc.PhoenixDriver
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset, Row, SaveMode, SparkSession}

import scala.collection.{mutable}

trait AbstractApplication extends Logging with Serializable {


  def setConf(conf: SparkConf, args: mutable.Map[String, String])

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
    setConf(sparkConf, args)
    sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
    // 本地执行不记录日志
    if (!"local".equalsIgnoreCase(master)) {
      // LogRecord.recordLog(sparkSession,args)
    }
    // 执行具体任务
    execute(sparkSession, args)
  }

  /**
   * 初始化spark conf
   *
   * @param appName 应用名称
   * @return
   */
  private def initSparkConf(appName: String): SparkConf = {
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
      .set("mapreduce.input.fileinputformat.split.maxsize", "256000000")
      // 执行 Map 前进行小文件合并
      .set("hive.merge.smallfiles.avgsize", "256000000")
      // 大小写不敏感
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.caseSensitive", "false")

  }

  /**
   * 获取数据源
   */
  protected def getSource(sparkSession: SparkSession, sourceType: String, sourceId: Long) = {
    val dataSource = new SimpleDataSource("ssxadmin")
    val props = dataSource.getConnProps()
    val result = sparkSession.read.jdbc(dataSource.getUrl, "source",  props)
      .where("status = 1")
      .where(s"source_id = $sourceId")
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
    if (result.isEmpty) {
      throw new RuntimeException("No Source Enable !!!")
    }
    result(0)
  }

  /**
   * 获取任务
   */
  protected def getJob(sparkSession: SparkSession, jobId: Long): Job = {
    val dataSource = new SimpleDataSource("ssxadmin")
    val props = dataSource.getConnProps()
    val result = sparkSession.read.jdbc(dataSource.getUrl, "job", props)
      .where("job_status = 0")
      .where(s"job_id = $jobId")
      .collect()
      .map { row => {
        val tmp = Job()
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
    if (result.isEmpty) {
      throw new RuntimeException("No Job Enable !!!")
    }
    result(0)
  }

  /**
   * 获取Phoenix链接
   */

  protected def getPhoenixConnect(url: String): Connection = {
    val driver = new PhoenixDriver()
    val properties = new Properties()
    val connect = driver.connect(url, properties)
    connect.setAutoCommit(false)
    connect.setReadOnly(false)
    connect
  }

  /**
   * 单Executor读取
   */
  protected def getDataFrame(sparkSession: SparkSession, source: Source, tableName: String): DataFrame = {
    sparkSession.read
      .format("jdbc")
      .option("url", source.url + "/" + source.dbName)
      .option("driver", source.driver)
      .option("user", source.userName)
      .option("password", source.password)
      .option("dbtable", tableName)
      .load
  }

  /**
   * 分片读取
   */
  protected def getDataFrame[T <: DataType](sparkSession: SparkSession, source: Source, tableName: String, splitKey: String, filter: String, dataType: Class[T]): DataFrame = {
    var lowerBound = ""
    var upperBound = ""
    var numPartitions = 0L
    if (dataType == classOf[LongType] || dataType == classOf[IntegerType]) {
      getDataFrame(sparkSession, source, tableName)
        .where(if (filter.trim.isEmpty) "1=1" else filter)
        .agg(splitKey -> "min", splitKey -> "max")
        .collect()
        .foreach(x => {
          lowerBound = x.get(0).toString
          upperBound = x.get(1).toString
          numPartitions = (x.get(1).toString.toLong - x.get(0).toString.toLong) / 300000L + 1
        })
    }
    if (dataType == classOf[TimestampType]) {
      getDataFrame(sparkSession, source, tableName)
        .where(if (filter.trim.isEmpty) "1=1" else filter)
        .agg(splitKey -> "min", splitKey -> "max")
        .collect()
        .foreach(x => {
          lowerBound = x.getTimestamp(0).toString.substring(0, 19)
          upperBound = x.getTimestamp(1).toString.substring(0, 19)
        })
      numPartitions = 32L
    }
    if (lowerBound == "" || upperBound == "") throw new Exception("source table is Empty")
    sparkSession.read
      .format("jdbc")
      .option("url", source.url + "/" + source.dbName)
      .option("driver", source.driver)
      .option("user", source.userName)
      .option("password", source.password)
      .option("dbtable", tableName)
      .option("partitionColumn", splitKey)
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .option("numPartitions", numPartitions)
      .option("fetchsize", 1000)
      .load
      .where(if (filter.trim.isEmpty) "1=1" else filter)
  }

  /**
   * 删除分区
   */
  protected def delPartition(sparkSession: SparkSession, tableName: String, partitionBy: JSONArray, jobParam: mutable.HashMap[String, String], dbName: String = "default"): Unit = {
    val sessionCatalog: SessionCatalog = sparkSession.sessionState.catalog
    var partitionSeq = Seq.empty[Map[String, String]]
    // 确认需要删除的分区
    partitionBy.toArray().map(_.asInstanceOf[JSONObject]).foreach(item => {
      val partitionStr = item.getObject(PartitionBy.PARTITION, classOf[String])
      val value = item.getObject(PartitionBy.VALUE, classOf[String])
      partitionSeq = partitionSeq :+ Map(partitionStr -> jobParam.get(value).getOrElse(value))
    })
    // 确认需要删除库表
    if (tableName.contains(".")) {
      val db = tableName.substring(0, tableName.indexOf("."))
      val table = tableName.substring(tableName.indexOf(".") + 1, tableName.length) //不存在时是否忽略                     //是否保留数据
      sessionCatalog.dropPartitions(new TableIdentifier(table, Option(db)), partitionSeq, true, true, false)
    } else {
      sessionCatalog.dropPartitions(new TableIdentifier(tableName, Option(dbName)), partitionSeq, true, true, false)
    }
  }

  /**
   * 返回splitKey类型
   */
  protected def getSplitKeyType(sparkSession: SparkSession, splitKey: String, source: Source, tableName: String) = {
    val structType = getDataFrame(sparkSession, source, tableName).select(splitKey).limit(1).schema
    val structFieldArray = structType.fields
    val structField = structFieldArray(0)
    val dataType: DataType = structField.dataType
    dataType match {
      case LongType => classOf[LongType]
      case IntegerType => classOf[IntegerType]
      case TimestampType => classOf[TimestampType]
      case _ => throw new Exception("Partition column type should be numeric, date, or timestamp")
    }
  }

  /**
   * 保存数据到ClickHouse，注意Overwrite模式，我们不删除表，只是清除数据，因为删除表存在权限问题
   */
  protected def save2Clickhouse(df: Dataset[Row], dbName: String, tableName: String, mode: SaveMode): Unit = {
    var tmpMode = SaveMode.Append
    if (null != mode) {
      tmpMode = mode
    }
    val dfWrite = df
      .write
      .format("jdbc")
      .mode(mode)
      .option("driver", "cc.blynk.clickhouse.ClickHouseDriver")
      .option("url", "jdbc:clickhouse://xxxxx:7001/test?rewriteBatchedStatements=true")
      .option("dbtable", tableName)
      .option("user", "")
      .option("password", "")
      .option("numPartitions", 4)
      .option("batchsize", 100000) //default 1000
    // 写mysql优化：配置numPartitions、batchsize，最关键的是url中配置rewriteBatchedStatements=true，即打开mysql的批处理能力
    log.info(dbName + "." + tableName + " 写入模式：" + mode)
    // 重写模式下，采用清空表的方式 ，注意，必须有drop的权限才可以，否则会一直报错没权限
    if (tmpMode eq SaveMode.Overwrite) dfWrite.option("truncate", true).save()
    else dfWrite.save()
  }

}
