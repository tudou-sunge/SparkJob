package com.ssx.spark.job


import com.ssx.spark.utils.ParseJobParam
import com.ssx.spark.{AbstractApplication, JobConsts}
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
 * @author sunshuxian
 * @version 1.0
 * @create 2021/05/10 10:53
 **/

class EtlDataExtract extends AbstractApplication {

  override def setConf(conf: SparkConf, args: mutable.Map[String, String]): Unit = {
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {
    val runDay = args(JobConsts.RUN_DAY)
    val seq = args(JobConsts.SEQ).toInt
    val jobId = args(JobConsts.JOB_ID).toLong
    logInfo(s"JobId: $jobId  runDay:$runDay  seq:$seq  Job Begin Running!!!!")
    val job = getJob(sparkSession, jobId)
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
    val source = getSource(sparkSession, sourceType, sourceId)
    val sourceTable = jobContent.getJSONArray("sourceTable")
    // 如果是覆盖则先删除分区内容
    if (jobContent.getObject("writeMode", classOf[String]) == "1") {
      delPartition(sparkSession, targetTable, partitionBy, jobParam, targetDb)
    }

    // 逐个便利要抽取的表写入HIVE中
    sourceTable.toArray().map(_.toString).foreach(t => {
      val splitKeyType = getSplitKeyType(sparkSession, splitKey, source, t)
      val tmpDF = getDataFrame(sparkSession, source, t, splitKey, filterStr, splitKeyType)
      transformDataFrame(tmpDF, sparkSession, fieldMapping, partitionBy, jobParam)
        .mode(SaveMode.Append)
        .saveAsTable(targetDb + "." + targetTable)
    })


  }

  /**
   * 对DF进行转译
   */
  override def transformDataFrame(df: DataFrame, sparkSession: SparkSession, fieldMapping: JSONArray, partitionBy: JSONArray, jobParam: mutable.HashMap[String, String]) = {
    var tmpDf = df
    // 要查询的字段
    var columnSeq = Seq.empty[String]
    // 分区字段
    var partitionSeq = Seq.empty[String]
    // 修改字段映射名称
    fieldMapping.toArray.map(_.asInstanceOf[JSONObject]).foreach(t => {
      val source = t.getObject(FieldMapping.SOURCE_NAME, classOf[String])
      val target = t.getObject(FieldMapping.TARGET_NAME, classOf[String])
      columnSeq = columnSeq :+ target
      tmpDf = tmpDf.withColumnRenamed(source, target)
    })

    // 增加分区字段及值
    partitionBy.toArray.map(_.asInstanceOf[JSONObject]).foreach(t => {
      val partition = t.getObject(PartitionBy.PARTITION, classOf[String])
      val value = t.getObject(PartitionBy.VALUE, classOf[String])
      columnSeq = columnSeq :+ partition
      partitionSeq = partitionSeq :+ partition
      tmpDf = tmpDf.withColumn(partition, functions.lit(ParseJobParam.replaceJobParam(jobParam, value)))
    })

    tmpDf.selectExpr(columnSeq: _*)
      .write
      .partitionBy(partitionSeq: _*)
      .format("hive")
  }

  // 测试方法
  def createTestData() = {
    //    val job = DataExtract()
    //    job.jobId = 55
    //    job.jobParam = "$yesterday={yyyyMMdd} $yesterdayiso={yyyy-MM-dd} $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd]"
    //    job.jobName = "ods_ceshi_chouqu1"
    //    job.jobType = "DataExtract"
    //    job.jobComment = "ods_ceshi_chouqu1"
    //    job.jobCycle = "D"
    //    job.jobExecuteTime = "00:05"
    //    job.jobContent = "{\"sourceType\": \"MYSQL\",\"sourceId\": null,\"sourceTable\": [\"da_app_page\"],\"targetType\": \"HIVE\",\"targetDb\": \"test\",\"targetTable\": \"da_app_page\",\"filter\": \"\",\"splitKey\": \"id\",\"writeMode\": 1,\"fieldMapping\" : [{ \"sourceName\": \"id\", \"targetName\": \"id\" },{ \"sourceName\": \"page\", \"targetName\": \"page\" },{ \"sourceName\": \"page_name\", \"targetName\": \"page_name\" },{ \"sourceName\": \"description\", \"targetName\": \"desc_col\" },{ \"sourceName\": \"status\", \"targetName\": \"status_cd\" }],\"partitionBy\": [{ \"partition\": \"d\", \"value\": \"$yesterday\" }]}"


    //    val source = Source()
    //    source.url = "jdbc:mysql://mysqln1.dabig.com:33061"
    //    source.driver = "com.mysql.jdbc.Driver"
    //    source.userName = "bigdata"
    //    source.password = "bigdata@mysql"
    //    source.dbName = "beacon"

    val job = DataExtract()
    job.jobId = 55
    job.jobParam = "$yesterday={yyyyMMdd} $yesterdayiso={yyyy-MM-dd} $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd]"
    job.jobName = "ods_ceshi_chouqu1"
    job.jobType = "DataExtract"
    job.jobComment = "ods_ceshi_chouqu1"
    job.jobCycle = "D"
    job.jobExecuteTime = "00:05"
    job.jobContent = "{\"sourceType\": \"MYSQL\",\"sourceId\": 1,\"sourceTable\": [\"source\"],\"targetType\": \"HIVE\",\"targetDb\": \"test\",\"targetTable\": \"da_app_page\",\"filter\": \"\",\"splitKey\": \"create_time\",\"writeMode\": 1,\"fieldMapping\" : [{ \"sourceName\": \"id\", \"targetName\": \"id\" },{ \"sourceName\": \"page\", \"targetName\": \"page\" },{ \"sourceName\": \"page_name\", \"targetName\": \"page_name\" },{ \"sourceName\": \"description\", \"targetName\": \"desc_col\" },{ \"sourceName\": \"status\", \"targetName\": \"status_cd\" }],\"partitionBy\": [{ \"partition\": \"d\", \"value\": \"$yesterday\" }]}"

    val source = Source()
    source.url = "jdbc:mysql://localhost:3306"
    source.driver = "com.mysql.cj.jdbc.Driver"
    source.userName = "root"
    source.password = "L2Bs9fD#"
    source.dbName = "ssxadmin"

    (job, source)
  }
}

