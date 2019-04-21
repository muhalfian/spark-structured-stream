package com.muhalfian.spark.util

import org.apache.spark.sql.types._

object ColsArtifact {
  val rawSchema : StructType = StructType(Seq(
    StructField("link", StringType,true),
    StructField("source", StringType, true),
    StructField("description", StringType, true),
    StructField("image", StringType, true),
    StructField("publish_date", StringType, true),
    StructField("title", StringType, true),
    StructField("text", StringType, true),
    StructField("text_html", StringType, true)
    )
  )

  val preprocessSchema : StructType = StructType(Seq(
    StructField("text_selected", ArrayType(StringType), true),
    StructField("group", IntegerType, true)
    )
  )

  case class masterData(
    link: String,
    source: String,
    description: String,
    image: String,
    publish_date: String,
    title: String,
    text: String,
    text_html: String,
    text_preprocess: String,
    text_selected: String,
    text_aggregate: String
  )

  case class masterDataUpdate(
    link: String,
    source: String,
    description: String,
    image: String,
    publish_date: String,
    title: String,
    text: String,
    text_html: String,
    text_preprocess: String,
    text_selected: String,
    text_aggregate: String,
    new_cluster: String,
    to_centroid: Double
  )

  case class dataInit(
    link: String,
    source: String,
    description: String,
    image: String,
    publish_date: String,
    title: String,
    text: String,
    text_html: String,
    text_preprocess: Array[String],
    text_selected: Array[(String, Double)]
  )
}
