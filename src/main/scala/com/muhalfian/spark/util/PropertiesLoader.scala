package com.muhalfian.spark.util

import com.typesafe.config.{Config, ConfigFactory}

object PropertiesLoader {
  private val conf : Config = ConfigFactory.load("application.conf")

  val kafkaBrokerUrl : String = conf.getString("KAFKA_BROKER_URL")
  val kafkaStartingOffset : String = conf.getString("KAFKA_STARTING_OFFSET")
  val kafkaTopic : String = conf.getString("KAFKA_TOPIC")

  val mongoUrl : String = conf.getString("MONGO_URL")
  val mongoDb : String = conf.getString("MONGO_DB")

  val dbDataInit = "data_init_7"
  val dbMasterWord = "master_word_7"
  val dbMasterData = "master_data_7"
  val dbMasterDataUpdate = "master_data_7"
  val dbMasterCluster = "master_cluster_backup_7"
  val dbMasterClusterUpdate = "master_cluster_7"
}
