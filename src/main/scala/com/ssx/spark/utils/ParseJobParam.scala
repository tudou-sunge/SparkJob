package com.ssx.spark.utils

import org.joda.time.DateTime

import scala.collection.mutable

object ParseJobParam {

  /**
   * 解析任务的参数
   */
  def parseJobParam(param: String, runDay: String, seq: Int) = {
    val result = new mutable.HashMap[String, String]()
    param.split(" ")
      .filter(!_.isEmpty).foreach(item => {
      if (item.contains("=")) {
        val key = item.substring(0, item.indexOf("="))
        val value = item.substring(item.indexOf("=") + 1, item.length)
        if (value.contains("{") || value.contains("[")) {
          result.put(key, parseBusinessTime(value, runDay, seq))
        } else {
          result.put(key, value)
        }
      } else {
        throw new Exception("任务参数不合法,需使用K=V的格式")
      }
    })
    result
  }

  /**
   * 将参数解析为要执行的业务时间
   */
  def parseBusinessTime(format: String, runDay: String, seq: Int): String = {
    var result = ""
    val runDate = new DateTime(runDay).plusHours(seq - 1 )
    if (format.contains("{")) {
      result = runDate.toString(format.replace("{", "").replace("}", ""))
    }
    if (format.contains("[")) {
      result = runDate.plusDays(1).toString(format.replace("[", "").replace("]", ""))
    }
    result
  }

  /**
   * 将各种字符串中的参数进行替换
   */
  def replaceJobParam(jobParam: mutable.HashMap[String, String], var1: String): String = {
    var result = var1
    if (var1.contains("$")) {
      jobParam.foreach {
        case (k, v) => {
          result = result.replace(k, v)
        }
      }
      result
    } else var1
  }

  def main(args: Array[String]): Unit = {
    val val1 = "$yesterday={yyyyMMdd}   $yesterdayiso={yyyy-MM-dd}  $today=[yyyyMMdd] $todayiso=[yyyy-MM-dd] $HH=[HH]"
    val val2 = "2021-04-25"
    val jobParam = parseJobParam(val1, val2, 2)
    val str = replaceJobParam(jobParam, "create_time>='$yesterdayiso' AND create_time < '$todayiso'")
    println(str)
  }
}
