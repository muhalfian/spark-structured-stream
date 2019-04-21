package com.muhalfian.spark.util

import com.typesafe.config.{Config, ConfigFactory}

object PropertiesLoader {
  private val conf : Config = ConfigFactory.load("application.conf")

  val kafkaBrokerUrl : String = conf.getString("KAFKA_BROKER_URL")
  val kafkaStartingOffset : String = conf.getString("KAFKA_STARTING_OFFSET")
  val kafkaTopic : String = conf.getString("KAFKA_TOPIC")

  val mongoUrl : String = conf.getString("MONGO_URL")
  val mongoDb : String = conf.getString("MONGO_DB")

  val dbDataInit = "data_init_7_temp"
  val dbMasterWord = "master_word_7_temp"
  val dbMasterData = "master_data_7_temp"
  val dbMasterDataUpdate = "master_data_7_update_temp"
  val dbMasterCluster = "master_cluster_7_temp"
  val dbMasterClusterUpdate = "master_cluster_7_update_temp"
}
