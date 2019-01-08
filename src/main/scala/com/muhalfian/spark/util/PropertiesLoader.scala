package com.muhalfian.spark.util

import com.typesafe.config.{Config, ConfigFactory}

object PropertiesLoader {
  private val conf : Config = ConfigFactory.load("application.conf")

  val kafkaBrokerUrl : String = conf.getString("KAFKA_BROKER_URL")
  val kafkaStartingOffset : String = conf.getString("KAFKA_STARTING_OFFSET")
  val kafkaTopic : String = conf.getString("KAFKA_TOPIC")

  val mongoUrl : String = conf.getString("MONGO_URL")
}
