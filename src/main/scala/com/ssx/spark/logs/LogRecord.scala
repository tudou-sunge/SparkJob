package com.ssx.spark.logs

import cn.hutool.db.{Db, Entity}
import cn.hutool.db.ds.simple.SimpleDataSource
import com.ssx.spark.JobConsts
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

//`id` bigint(11) NOT NULL AUTO_INCREMENT,
//`job_id` bigint(20) DEFAULT NULL COMMENT '任务ID',
//`seq` int DEFAULT 1 NULL COMMENT '周期任务序号',
//`run_day` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '任务运行日',
//`create_time` DATETIME  DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
//`application_id` varchar(255) COMMENT '运行的Spark实例ID',

object LogRecord {

  val LOG_TABLE = "job_run_log"
  /**
   * 记录日志，及该任务的应用ID
   */
  def recordLog(sparkSession: SparkSession, args: mutable.Map[String, String]) = {
    val jobId = args(JobConsts.JOB_ID).toLong
    val runDay = args(JobConsts.RUN_DAY)
    val seq = args(JobConsts.SEQ).toInt
    val dataSource = new SimpleDataSource(JobConsts.PROJECT_NAME)
    Db.use(dataSource).insert(
      Entity.create(LOG_TABLE)
        .set("job_id", jobId)
        .set("seq", seq)
        .set("run_day", runDay)
        .set("application_id", sparkSession.sparkContext.applicationId)
    )
  }
}
