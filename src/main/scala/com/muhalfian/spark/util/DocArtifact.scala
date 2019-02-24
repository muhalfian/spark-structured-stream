package com.muhalfian.spark.util

import org.bson.Document

object DocArtifact {

  def dataInit(r:Document) = {
    ColsArtifact.dataInit(
      r.get("link", String),
      r.get("source", String),
      r.get("description", String),
      r.get("image", String),
      r.get("publish_date", String),
      r.get("title", String),
      r.get("text", String),
      r.get("text_preprocess", Array[String]),
      r.get("text_selected", Array[(String, Double)])
    )
  }
}
