package com.ssx.spark.job

import java.util.function.Consumer

import com.ssx.spark.utils.ParseJobParam
import com.ssx.spark.AbstractApplication
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.ssx.spark.common.{DataExtract, Source}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql._

import scala.collection.mutable

/**
 * 抽数任务
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.job.EtlDataExtract,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @author sunshuxian@dongao.com
 * @version 1.0    只支持天分区
 * @create 2020/06/27 10:53
 **/

class EtlDataExtract extends AbstractApplication {
  override def setConf(conf: SparkConf): Unit = {
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {
    val runDay = args("runDay")
    val seq = args("seq").toInt
    val jobId = args("jobId").toLong
    // val job = getJob(sparkSession, jobId)
    val (job, source) = createTestData()
    val jobParam = ParseJobParam.parseJobParam(job.jobParam, runDay, seq)
    val jobContent = JSON.parseObject(job.jobContent)
    val sourceType = jobContent.getObject("sourceType", classOf[String])
    val sourceId = jobContent.getObject("sourceId", classOf[Long])
    val targetDb = jobContent.getObject("targetDb", classOf[String])
    val targetTable = jobContent.getObject("targetTable", classOf[String])
    val filter = jobContent.getObject("filter", classOf[String])
    val filterStr = ParseJobParam.replaceJobParam(jobParam, filter)
    val splitKey = jobContent.getObject("splitKey", classOf[String])
    val fieldMapping = jobContent.getJSONArray("fieldMapping")
    val partitionBy = jobContent.getJSONArray("partitionBy")
    // val source = getSource(sparkSession, sourceType, sourceId)
    val sourceTable = jobContent.getJSONArray("sourceTable")
    // 如果是覆盖则先删除分区内容
    if (jobContent.getObject("writeMode", classOf[String]) == "1") {
      delPartition(sparkSession, targetTable, partitionBy, jobParam, targetDb)
    }

    sourceTable.toArray().map(_.toString).foreach(t => {
      val tmpDF = getDataFrame(sparkSession, source, t, splitKey, filterStr)
      transformDataFrame(tmpDF, sparkSession, fieldMapping, partitionBy, jobParam)
        .mode(SaveMode.Append)
        .saveAsTable(targetDb + "." + targetTable)
    })


  }


  def transformDataFrame(df: DataFrame, sparkSession: SparkSession, fieldMapping: JSONArray, partitionBy: JSONArray, jobParam: mutable.HashMap[String, String]) = {
    var tmpDf = df
    // 要查询的字段
    var columnSeq = Seq.empty[String]
    var partitionSeq = Seq.empty[String]
    // 修改字段别名
    fieldMapping.toArray.map(_.asInstanceOf[JSONObject]).foreach(t => {
      val source = t.getObject("sourceName", classOf[String])
      val target = t.getObject("targetName", classOf[String])
      columnSeq = columnSeq :+ target
      tmpDf = tmpDf.withColumnRenamed(source, target)
    })

    // colNames: Seq[String], cols: Seq[Column]
    partitionBy.toArray.map(_.asInstanceOf[JSONObject]).foreach(t => {
      val partition = t.getObject("partition", classOf[String])
      val value = t.getObject("value", classOf[String])
      columnSeq = columnSeq :+ partition
      partitionSeq = partitionSeq :+ partition
      tmpDf = tmpDf.withColumn(partition, functions.lit(ParseJobParam.replaceJobParam(jobParam, value)))
    })

    tmpDf.selectExpr(columnSeq: _*)
      .write
      .partitionBy(partitionSeq: _*)
      .format("hive")
  }

  /**
   * 单Executor读取
   */
  private def getDataFrame(sparkSession: SparkSession, source: Source, tableName: String): DataFrame = {
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
  private def getDataFrame(sparkSession: SparkSession, source: Source, tableName: String, splitKey: String, filter: String): DataFrame = {
    var lowerBound = 1L
    var upperBound = 1L
    this.getDataFrame(sparkSession, source, tableName)
      .where(if (filter.trim.isEmpty) "1=1" else filter)
      .agg(splitKey -> "min", splitKey -> "max")
      .collect()
      .foreach(x => {
        lowerBound = x.get(0).toString.toLong
        upperBound = x.get(1).toString.toLong + 1
      })
    val numPartitions = (upperBound - lowerBound) / 300000L + 1
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
  private def delPartition(sparkSession: SparkSession, tableName: String, partitionBy: JSONArray, jobParam: mutable.HashMap[String, String], dbName: String = "default"): Unit = {
    val sessionCatalog: SessionCatalog = sparkSession.sessionState.catalog
    var partitionSeq = Seq.empty[Map[String, String]]
    // 确认需要删除的分区
    partitionBy.toArray().map(_.asInstanceOf[JSONObject]).foreach(item => {
      val partitionStr = item.getObject("partition", classOf[String])
      val value = item.getObject("value", classOf[String])
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

  // 测试方法
  def createTestData() = {
    val job = DataExtract()
    job.jobId = 55
    job.jobParam = "$yesterday={yyyyMMdd} $yesterdayiso={yyyy-MM-dd} $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd]"
    job.jobName = "ods_ceshi_chouqu1"
    job.jobType = "DataExtract"
    job.jobComment = "ods_ceshi_chouqu1"
    job.jobCycle = "D"
    job.jobExecuteTime = "00:05"
    job.jobContent = "{\"sourceType\": \"MYSQL\",\"sourceId\": null,\"sourceTable\": [\"da_app_page\"],\"targetType\": \"HIVE\",\"targetDb\": \"test\",\"targetTable\": \"da_app_page\",\"filter\": \"\",\"splitKey\": \"id\",\"writeMode\": 1,\"fieldMapping\" : [{ \"sourceName\": \"id\", \"targetName\": \"id\" },{ \"sourceName\": \"page\", \"targetName\": \"page\" },{ \"sourceName\": \"page_name\", \"targetName\": \"page_name\" },{ \"sourceName\": \"description\", \"targetName\": \"desc_col\" },{ \"sourceName\": \"status\", \"targetName\": \"status_cd\" }],\"partitionBy\": [{ \"partition\": \"d\", \"value\": \"$yesterday\" }]}"

    val source = Source()
    source.url = "jdbc:mysql://mysqln1.dabig.com:33061"
    source.driver = "com.mysql.jdbc.Driver"
    source.userName = "bigdata"
    source.password = "bigdata@mysql"
    source.dbName = "beacon"

    (job, source)
  }

}


