package com.ssx.spark.demo

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.Pattern

import com.alibaba.fastjson.JSONObject
import com.ssx.spark.{AbstractApplication, JobConsts}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * 解析Nginx日志Demo
 * 调用示例：
 * spark-submit --class com.ssx.spark.SparkEntry --master yarn --deploy-mode cluster /home/sunshuxian/jar/spark-job.jar "className=com.ssx.spark.demo.TestNginx,runDay=2021-01-01,seq=1,jobId=55"
 *
 * @author sunshuxian
 * @version 1.0
 * @create 2021/05/10 10:53
 **/
class TestNginx extends AbstractApplication{

  override def setConf(conf: SparkConf, args: mutable.Map[String, String]): Unit = {
    conf.set("spark.executor.memory", "2G")
  }

  override def execute(sparkSession: SparkSession, args: mutable.Map[String, String]): Unit = {
    val targetDay = args.get(JobConsts.RUN_DAY).get
    val path = s"/nginx/logs/d=$targetDay/"
    //val path = "/tmp/ngnix/"

    val formatter = new SimpleDateFormat("[dd/MMM/yyyy:hh:mm:ss Z]", Locale.ENGLISH)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    val p = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) (\\[.*\\]) (\\\".*?\\\") (-|[0-9]*) (-|[0-9]*) ([^ ]*) (\\\".*?\\\") (\\\".*?\\\") (\\\".*?\\\") (\\\".*?\\\")")
    val txtRdd = sparkSession.sparkContext.textFile(path)
    val logRdd = txtRdd.repartition(50).filter(x => {
      if (x.isEmpty) false
      else {
        if (x.contains("\"POST")) false
        else true
      }
    }).map(x => {
      val m = p.matcher(x)
      if (m.find()) {
        try{
          def parse(var1:String):String ={
            val jsonObject=new JSONObject()
            if (var1.split("\\?",2).length== 2) {
              val tmpstr1=var1.split("\\?",2)(1)
              for(item <- tmpstr1.split("&")){
                if (item.contains("=")){
                  val key=item.substring(0,item.indexOf("=")).toLowerCase
                  val value=item.substring(item.indexOf("=")+1,item.length)
                  jsonObject.put(key,value)
                }else
                  jsonObject.put(item.toLowerCase,"")
              }
              jsonObject.toString
            } else  ""
          }
          val request = URLDecoder.decode(m.group(5).replaceAll("\"", ""), "UTF-8")
          val referrer = URLDecoder.decode(m.group(9).replaceAll("\"", ""), "UTF-8")
          val create_tm = dateFormat.format(formatter.parse(m.group(4)))
          val param = parse(request.substring(1, request.length - 9))
          val response_time = if (m.group(12) == "\"-\"") 0.0 else {
            m.group(12).replaceAll("\"", "").split(",").collect{case x if x.trim !="-"=> x.toDouble}.reduce(_ + _)
          }
          AccessLog(m.group(1), m.group(2), m.group(3), create_tm, request, m.group(6).toInt,
            m.group(7).toLong,m.group(8).toDouble, referrer, m.group(10).replaceAll("\"", ""), m.group(11).replaceAll("\"", "")
            , response_time
            , param)
        } catch {
          case e: Exception => {
            println(x)
            AccessLog()
          }
        }
      } else AccessLog()
    })

    import sparkSession.implicits._
    logRdd.toDF().createTempView("tmp_nginx_log_view")
    val sql = s"CREATE TABLE test.test1 AS SELECT * FROM tmp_nginx_log_view \n WHERE ip!=''"
    log.info("插入SQL：" + sql)
    sparkSession.sql(sql)
  }


}

case class AccessLog(
                      ip: String = "", //设备用户的真实ip地址
                      clientIdent: String = "", //用户标识
                      userId: String = "", //用户
                      timestamp: String = "", //访问日期时间
                      request: String = "", //请求信息，get/post，mac值等
                      responseCode: Int = 0, //请求状态 200成功，304静态加载
                      contentSize: Long = 0L, //返回文件的大小
                      request_time: Double = 0.0, //请求时间
                      referrer: String = "", //跳转来源
                      agent: String = "", //UA信息
                      http_x_forwarded_for: String = "",
                      response_time: Double = 0.0, //返回时间
                      param: String = "" //请求参数
                    )
