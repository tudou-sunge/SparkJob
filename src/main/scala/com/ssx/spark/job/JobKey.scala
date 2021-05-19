package com.ssx.spark.job

object JobKey {
    // 数据源类型
    val SOURCE_TYPE = "sourceType"
    // 数据源ID
    val SOURCE_ID  = "sourceId"
    // 源库
    val SOURCE_DB = "sourceDb"
    // 数据源表
    val SOURCE_TABLE = "sourceTable"
    // 目标类型
    val TARGET_TYPE = "targetType"
    // 目标库ID
    val TARGET_ID = "targetId"
    // 目标库
    val TARGET_DB = "targetDb"
    // 目标表
    val TARGET_TABLE  = "targetTable"
    // 过滤条件
    val FILTER = "filter"
    // 分片KEY
    val SPLIT_KEY = "splitKey"
    // 字段映射
    val FIELD_MAPPING = "fieldMapping"
    // 分区
    val PARTITION_BY = "partitionBy"
    // 写入模式
    val WRITE_MODE = "writeMode"
    // SQL列表
    val SQL_LIST = "sqlList"
    // 执行插入之前执行的SQL
    val BEFORE_EXECUTE_SQL = "beforeExecuteSQL"
    // 执行插入之后执行的SQL
    val AFTER_EXECUTE_SQL = "afterExecuteSQL"

}
