package com.ssx.spark.utils

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.partitioner.{BsonValueOrdering, MongoPaginateByCountPartitioner, MongoPartition, PartitionerHelper}
import org.bson.{BsonDocument, BsonObjectId}
import org.bson.types.ObjectId


/**
 * 自定义分片规则
 * @description
 * @author sunshuxian
 * @createTime 2021/5/11 6:32 下午
 * @version 1.0
 */
private[utils] class MongoPartitionUserDefined extends MongoPaginateByCountPartitioner {
  private implicit object BsonValueOrdering extends BsonValueOrdering
  private val DefaultPartitionKey = "_id"
  private val DefaultNumberOfPartitions = "64"

  override def partitions(connector: MongoConnector, readConfig: ReadConfig, pipeline: Array[BsonDocument]): Array[MongoPartition] = {
    val boundaryQuery = PartitionerHelper.createBoundaryQuery("_id", new BsonObjectId(new ObjectId("5fedf5800000000000000000")), new BsonObjectId(new ObjectId("5fee03900000000000000000")))
    val mongoPartition = MongoPartition(0, boundaryQuery, PartitionerHelper.locations(connector))
    // TODO
    Array[MongoPartition](mongoPartition)
  }
}


case object MongoPartitionUserDefined extends MongoPartitionUserDefined
