package com.ssx.spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

import scala.collection.{immutable, mutable}

object SparkEntry extends Logging {

  def main(args: Array[String]): Unit = {
    val params = new mutable.HashMap[String, String]
    if (args.length < 1) throw new IllegalArgumentException(String.valueOf("className is required"))
    // 格式化参数集
    initParams(params, args)
    if (StringUtils.isBlank(params.get(JobConsts.ARGS_CLASS_NAME).getOrElse(""))) throw new IllegalArgumentException(String.valueOf("className is required"))
    val className: String = params.get(JobConsts.ARGS_CLASS_NAME).get
    val o = Class.forName(className).newInstance()
    if (!o.isInstanceOf[AbstractApplication]) throw new IllegalArgumentException(String.valueOf(className + " is not a subClass of AbstractApplication"))
    val abstractApplication = o.asInstanceOf[AbstractApplication]
    abstractApplication.execute(params)
  }


  /**
   * 将传入的参数放入map<br>
   * 规定通过脚本只传递一个参数<br>
   * 多参数逗号分隔<br>
   * 每个参数形式： k=v
   *
   * @param params 格式化后的参数集
   * @param args   传入的参数
   */
  def initParams(params: mutable.Map[String, String], args: Array[String]) {
    if (args != null && args.length >= 1) {
      if (StringUtils.isNotBlank(args(0))) {
        val execParams = args(0).split(",")
        for (execParam <- execParams) {
          if (StringUtils.isNotBlank(execParam)) {
            val split = execParam.split("=");
            if (split.length == 2 && StringUtils.isNotBlank(split(0))) params += (split(0) -> split(1))
          }
        }
      }
    }
  }

}
