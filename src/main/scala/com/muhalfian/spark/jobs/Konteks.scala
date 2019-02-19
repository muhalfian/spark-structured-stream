package com.muhalfian.spark.jobs

import org.apache.spark.{ SparkConf, SparkContext, _}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import org.apache.kafka.clients.admin.{AdminClient,NewTopic,AdminClientConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties
import java.time.Instant
import collection.JavaConverters._
import collection.JavaConversions._
import scala.util.parsing.json._
import scala.util.Try

object Konteks {
  val APP_NAME = "BatchProcessKafka"
  val LOG = LoggerFactory.getLogger(Konteks.getClass);

  def main(args : Array[String]){

    // val appName = s"${this.getClass().getSimpleName},maxCores,${maxCores},rows:${rows}:dim:${dimension},"
    // val conf = new SparkConf()
    //     .setAppName(appName)
    //     .setMaster(master)
    //     .set("spark.cores.max", maxCores)
    // val sc = new SparkContext(conf)

    // =======================================================

    val inputTopic = "online_media"
    val kafkaBrokers = "ubuntu:9092"

    val conf = new SparkConf().setAppName(APP_NAME)
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder.config(conf).appName(APP_NAME).getOrCreate()
    val sqlContext = sparkSession.sqlContext
    import sparkSession.implicits._

    val gid = APP_NAME + Instant.now.getEpochSecond

    val kafkaParams = scala.collection.immutable.Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> gid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ).asJava

    val KafkaConfig = new Properties()
    KafkaConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    val adminClient = AdminClient.create(KafkaConfig)

    val consumer = new KafkaConsumer[String,String](kafkaParams)

    // Get list of all partitions of given Topic
    val topicPartitions = adminClient
                          .describeTopics(List[String](inputTopic).asJava)
                          .all().get().get(inputTopic).partitions()

    // Create Array of OffsetRange with topic, partition number, start & end offsets
    val offsetRanges = topicPartitions.asScala.map(x =>{
      val topicPartition = new TopicPartition(inputTopic, x.partition)
      val startOffset = consumer.beginningOffsets(List[TopicPartition](topicPartition))
                        .values().asScala.toList.get(0)
      val stopOffset = consumer.endOffsets(List[TopicPartition](topicPartition))
                        .values().asScala.toList.get(0)
      OffsetRange(topicPartition,
                    startOffset,
                    stopOffset)
    }).toArray

    // Create RDD from provided topic & offset details
    val messagesRDD = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, PreferConsistent)

    print(messagesRDD)
  }
}
