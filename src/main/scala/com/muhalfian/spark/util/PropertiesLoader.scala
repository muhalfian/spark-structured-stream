package com.muhalfian.spark.util

import com.typesafe.config.{Config, ConfigFactory}

object PropertiesLoader {
  private val conf : Config = ConfigFactory.load("application.conf")

  val kafkaBrokerUrl : String = conf.getString("KAFKA_BROKER_URL")
  val kafkaStartingOffset : String = conf.getString("KAFKA_STARTING_OFFSET")
  val kafkaTopic : String = conf.getString("KAFKA_TOPIC")

  val mongoUrl : String = conf.getString("MONGO_URL")
  val mongoDb : String = conf.getString("MONGO_DB")

  val dbDataInit = "data_init"
  val dbMasterWord = "master_word"
  val dbMasterData = "master_data"
  val dbMasterDistance = "master_distance"
  val dbMasterDataUpdate = "master_data"
  val dbMasterCluster = "master_cluster_backup"
  val dbMasterClusterUpdate = "master_cluster"
}
