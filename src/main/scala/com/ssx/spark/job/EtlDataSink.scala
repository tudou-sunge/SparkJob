package com.ssx.spark.job

import cn.hutool.db.ds.simple.SimpleDataSource
import cn.hutool.db.{Db, Entity}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.ssx.spark.common.{Job, Source}
import com.ssx.spark.utils.ParseJobParam
import com.ssx.spark.{AbstractApplication, JobConsts}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession, functions}

import scala.collection.mutable

/**
 * 推送任务
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.job.EtlDataSink,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @description
 * @author sunshuxian
 * @createTime 2021/5/11 6:14 下午
 * @version 1.0
 */
class EtlDataSink extends AbstractApplication {
  var sourceDb: String = _
  var sourceTable: String = _
  var targetId: Long = _
  var targetType: String = _
  var targetTable: String = _

  override def setConf(conf: SparkConf, args: mutable.Map[String, String]): Unit = {
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {
    val runDay = args(JobConsts.RUN_DAY)
    val seq = args(JobConsts.SEQ).toInt
    val jobId = args(JobConsts.JOB_ID).toLong
    logInfo(s"JobId: $jobId  runDay:$runDay  seq:$seq  Job Begin Running!!!!")
    // val (job, source) = createTestData()

    val job = getJob(sparkSession, jobId)
    val jobParam = ParseJobParam.parseJobParam(job.jobParam, runDay, seq)
    val jobContent = JSON.parseObject(job.jobContent)
    sourceDb = jobContent.getObject(JobKey.SOURCE_DB, classOf[String])
    sourceTable = jobContent.getObject(JobKey.SOURCE_TABLE, classOf[String])
    targetType = jobContent.getObject(JobKey.TARGET_TYPE, classOf[String])
    targetId = jobContent.getObject(JobKey.TARGET_ID, classOf[Long])
    targetTable = jobContent.getObject(JobKey.TARGET_TABLE, classOf[String])
    val beforeExecuteSQL = ParseJobParam.replaceJobParam(jobParam, jobContent.getObject(JobKey.BEFORE_EXECUTE_SQL, classOf[String]))
    val afterExecuteSQL = ParseJobParam.replaceJobParam(jobParam, jobContent.getObject(JobKey.AFTER_EXECUTE_SQL, classOf[String]))
    val fieldMapping = jobContent.getJSONArray(JobKey.FIELD_MAPPING)
    val partitionBy = jobContent.getJSONArray(JobKey.PARTITION_BY)
    val source = getSource(sparkSession, targetType, targetId)

    // 如果不为空则执行
    if (StringUtils.isNotBlank(beforeExecuteSQL)) {
      executeSql(source, targetTable, beforeExecuteSQL)
    }

    val df = sparkSession.table(sourceDb + "." + sourceTable)
    val dfWrite = transformDataFrame(df, sparkSession, fieldMapping, partitionBy, jobParam)
    save(dfWrite, source, targetTable)

    // 如果不为空则执行
    if (StringUtils.isNotBlank(afterExecuteSQL)) {
      executeSql(source, targetTable, afterExecuteSQL)
    }

  }

  /**
   * 对DF进行转译
   */
  def transformDataFrame(df: DataFrame,
                         sparkSession: SparkSession, fieldMapping: JSONArray,
                         partitionBy: JSONArray, jobParam: mutable.HashMap[String, String]
                        ) = {
    val catalog = sparkSession.sessionState.catalog
    val catalogTable = catalog.externalCatalog.getTable(sourceDb, sourceTable)
    val partitionColumnNames = catalogTable.partitionColumnNames
    log.info("日志： " + partitionColumnNames)
    // 是否是分区表
    var isPartitionTable = true
    if ( partitionColumnNames.isEmpty) {
      isPartitionTable = false
    }

    var tmpDf = df
    // 要查询的字段
    var columnSeq = Seq.empty[String]
    // 过滤分区条件
    var filterStr = ""
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
      filterStr = filterStr + partition + "='" + ParseJobParam.replaceJobParam(jobParam, value) + "' AND "
    })

    filterStr = filterStr + " 1=1 "

    tmpDf.selectExpr(columnSeq: _*)
      .where(if(isPartitionTable) filterStr else "1=1")
  }

  /**
   * 执行Spark任务前后对目标库执行的SQL
   */
  def executeSql(source: Source, tableName: String, sql: String): Unit = {
    val url = source.url + "/" + source.dbName
    val dataSource = new SimpleDataSource(url, source.userName, source.password, source.driver)
    Db.use(dataSource).execute(sql, null)
  }

  /**
   * 保存
   */
  def save(df: DataFrame, source: Source, tableName: String): Unit = {
    df.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("driver", source.driver)
      .option("url", source.url + "/" + source.dbName + "?rewriteBatchedStatements=true")
      .option("dbtable", tableName)
      .option("user", source.userName)
      .option("password", source.password)
      .option("numPartitions", 3)
      .option("batchsize", 2000)
      .save()
  }

  // 测试方法
  def createTestData() = {
    val job = Job()
    job.jobId = 55
    job.jobParam = "$yesterday={yyyyMMdd} $yesterdayiso={yyyy-MM-dd} $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd]"
    job.jobName = "ads_test_sink"
    job.jobType = "DataSink"
    job.jobComment = "ads_test_sink"
    job.jobCycle = "D"
    job.jobExecuteTime = "00:05"
    job.jobContent = "{\"sourceType\": \"HIVE\",\"sourceDb\": \"dim\",\"sourceTable\": \"biz_base_season\",\"targetType\": \"MYSQL\",\"targetId\": 1,\"targetTable\": \"tmp_beh_qa_event_sum_d\",\"beforeExecuteSQL\": \"\",\"afterExecuteSQL\": \"\",\"fieldMapping\": [{\"sourceName\": \"exam_id\",\"targetName\": \"exam_id\"}, {\"sourceName\": \"exam_name\",\"targetName\": \"exam_name\"}, {\"sourceName\": \"subject_id\",\"targetName\": \"subject_id\"}, {\"sourceName\": \"subject_name\",\"targetName\": \"subject_name\"}, {\"sourceName\": \"start_date\",\"targetName\": \"start_tm\"}],\"partitionBy\": [{\"partition\": \"d\",\"value\": \"$yesterday\"}]}"


    val source = Source()
    source.url = "jdbc:mysql://mysqln1.dabig.com:33061"
    source.driver = "com.mysql.jdbc.Driver"
    source.userName = "bigdata"
    source.password = "bigdata@mysql"
    source.dbName = "beacon"

    //    val job = Job()
    //    job.jobId = 55
    //    job.jobParam = "$yesterday={yyyyMMdd} $yesterdayiso={yyyy-MM-dd} $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd]"
    //    job.jobName = "ods_ceshi_chouqu1"
    //    job.jobType = "DataExtract"
    //    job.jobComment = "ods_ceshi_chouqu1"
    //    job.jobCycle = "D"
    //    job.jobExecuteTime = "00:05"
    //    job.jobContent = "{\"sourceType\": \"MYSQL\",\"sourceId\": 1,\"sourceTable\": [\"source\"],\"targetType\": \"HIVE\",\"targetDb\": \"test\",\"targetTable\": \"da_app_page\",\"filter\": \"\",\"splitKey\": \"create_time\",\"writeMode\": 1,\"fieldMapping\" : [{ \"sourceName\": \"id\", \"targetName\": \"id\" },{ \"sourceName\": \"page\", \"targetName\": \"page\" },{ \"sourceName\": \"page_name\", \"targetName\": \"page_name\" },{ \"sourceName\": \"description\", \"targetName\": \"desc_col\" },{ \"sourceName\": \"status\", \"targetName\": \"status_cd\" }],\"partitionBy\": [{ \"partition\": \"d\", \"value\": \"$yesterday\" }]}"
    //
    //    val source = Source()
    //    source.url = "jdbc:mysql://localhost:3306"
    //    source.driver = "com.mysql.cj.jdbc.Driver"
    //    source.userName = "root"
    //    source.password = "L2Bs9fD#"
    //    source.dbName = "ssxadmin"

    (job, source)
  }

}

