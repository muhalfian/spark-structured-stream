package com.muhalfian.spark.util

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.ForeachWriter
import org.bson._
import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet}
import scala.collection.JavaConverters._

object WriterUtil {

  val masterData = new ForeachWriter[ColsArtifact.masterData] {
    var masterCollection : String = PropertiesLoader.mongoUrl + "prayuga.master_data"
    val writeConfig: WriteConfig = WriteConfig(Map("uri" -> masterCollection))
    var mongoConnector: MongoConnector = _
    var masterDataCounts: ArrayBuffer[ColsArtifact.masterData] = _

    override def process(value: ColsArtifact.masterData): Unit = {
      masterDataCounts.append(value)
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (masterDataCounts.nonEmpty) {
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          collection.insertMany(masterDataCounts.map(sc => {
            var doc = new Document()
            doc.put("link", sc.link)
            doc.put("source", sc.source)
            doc.put("authors", sc.authors)
            doc.put("image", sc.authors)
            doc.put("publish_date", sc.publish_date)
            doc.put("title", sc.title)
            doc.put("text", sc.text)
            doc.put("text_preprocess", sc.text_preprocess)
            doc.put("text_aggregate", sc.text_aggregate)
            doc.put("text_selected", sc.text_selected)
            doc
          }).asJava)
        })
      }
    }

    override def open(partitionId: Long, version: Long): Boolean = {
      mongoConnector = MongoConnector(writeConfig.asOptions)
      masterDataCounts = new ArrayBuffer[ColsArtifact.masterData]()
      true
    }
  }
}
