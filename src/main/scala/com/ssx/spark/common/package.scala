package com.ssx.spark
import java.sql.Timestamp
package object common {

  case class DataExtract(var jobId: Long = 0L,
                         var jobName: String = "",
                         var jobType: String = "",
                         var jobComment: String = "",
                         var jobParam: String = "",
                         var jobCycle: String = "",
                         var jobExecuteTime: String = "",
                         var jobSonType: Int = 0,
                         var jobContent: String = "",
                         var createBy: String = "",
                         var createTime: Timestamp = new Timestamp(0),
                         var updateBy: String = "",
                         var updateTime: Timestamp = new Timestamp(0),
                         var isRetry: Int = 0,
                         var jobOwner: String = "",
                         var jobAlias: String = "",
                         var jobStatus: Int = 0,
                         var jobWeight: Int = 0
                        )
  case class Source(var sourceId: Long = 0L,
                    var sourceName: String = "",
                    var sourceComment: String = "",
                    var sourceType: String = "",
                    var url: String = "",
                    var dbName: String = "",
                    var driver: String = "",
                    var userName: String = "",
                    var password: String = "",
                    var status: Int = 0,
                    var createBy: String = "",
                    var createTime: Timestamp = new Timestamp(0),
                    var updateBy: String = "",
                    var updateTime: Timestamp = new Timestamp(0)
                   )
}
