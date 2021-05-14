package com.ssx.spark.job

import com.alibaba.fastjson.JSONArray
import com.ssx.spark.AbstractApplication
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.collection.mutable

;

/**
 * @description
 * @author sunshuxian
 * @createTime 2021/5/11 6:14 下午
 * @version 1.0
 */
class EtlDataSink extends AbstractApplication{

  override def setConf(conf: SparkConf, args: mutable.Map[String, String]): Unit = {
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {

  }

  /**
   * 对DF进行转译
   */
  override def transformDataFrame(df: DataFrame, sparkSession: SparkSession, fieldMapping: JSONArray, partitionBy: JSONArray, jobParam: mutable.HashMap[String, String]): DataFrameWriter[Row] = {
    null
  }
}
