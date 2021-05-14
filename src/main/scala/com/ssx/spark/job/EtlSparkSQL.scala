package com.ssx.spark.job

import com.alibaba.fastjson.JSON
import com.ssx.spark.AbstractApplication
import com.ssx.spark.utils.ParseJobParam
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.ssx.spark.common.{DataExtract, Source}

import scala.collection.mutable

/**
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.job.EtlSparkSQL,runDay=2021-05-07,seq=1,jobId=55"
 * @description
 * @author sunshuxian
 * @createTime 2021/4/30 11:55 上午
 * @version 1.0
 */
class EtlSparkSQL extends AbstractApplication{

  override def setConf(conf: SparkConf, args: mutable.Map[String, String]): Unit = {
    conf.set("spark.executor.memory", "4G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {
    val runDay = args("runDay")
    val seq = args("seq").toInt
    val jobId = args("jobId").toLong
    val job = getJob(sparkSession, jobId)
    logInfo(s"JobId: $jobId  runDay:$runDay  seq:$seq  Job Begin Running!!!!")
    val jobParam = ParseJobParam.parseJobParam(job.jobParam, runDay, seq)
    val jobContent = JSON.parseObject(job.jobContent)
    val sqlList = jobContent.getJSONArray("sqlList")
    sqlList.toArray().map(_.toString).foreach(item => {
      val sql = ParseJobParam.replaceJobParam(jobParam, item)
      logInfo(sql)
      sparkSession.sql(sql)
    })
  }

  // 测试方法
  def createTestData() = {
    val job = DataExtract()
    job.jobId = 55
    job.jobName = "dwd_test_sql"
    job.jobType = "DataExtract"
    job.jobComment = "dwd_test_sql"
    job.jobCycle = "D"
    job.jobExecuteTime = "00:05"
    job.jobParam = "$yesterday={yyyyMMdd} $yesterdayiso={yyyy-MM-dd} $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd]"
    job.jobContent = "{\"sqlList\":[\"create table test.test1 as select * from dim.biz_base_exam\",\"create table test.test2 as select * from dim.biz_base_exam\"]}"

    val source = Source()

    (job, source)
  }

}
