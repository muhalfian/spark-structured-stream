package com.muhalfian.spark.util

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.ForeachWriter
import org.bson._
import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet, WrappedArray}
import scala.collection.JavaConverters._
import com.muhalfian.spark.util._
import org.apache.spark.rdd.RDD

import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

object WriterUtil {

  val uri = PropertiesLoader.mongoUrl
  val db = PropertiesLoader.mongoDb

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
            doc.put("description", sc.description)
            doc.put("image", sc.image)
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

  val masterDataUpdate = new ForeachWriter[ColsArtifact.masterDataUpdate] {
    var masterCollection : String = PropertiesLoader.mongoUrl + PropertiesLoader.mongoDb + "." + PropertiesLoader.dbMasterDataUpdate
    val writeConfig: WriteConfig = WriteConfig(Map("uri" -> masterCollection))
    var mongoConnector: MongoConnector = _
    var masterDataCounts: ArrayBuffer[ColsArtifact.masterDataUpdate] = _

    override def process(value: ColsArtifact.masterDataUpdate): Unit = {
      masterDataCounts.append(value)
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (masterDataCounts.nonEmpty) {
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          collection.insertMany(masterDataCounts.map(sc => {
            var doc = new Document()
            doc.put("link", sc.link)
            doc.put("source", sc.source)
            doc.put("description", sc.description)
            doc.put("image", sc.image)
            doc.put("publish_date", sc.publish_date)
            doc.put("title", sc.title)
            doc.put("text", sc.text)
            doc.put("text_html", sc.text)
            doc.put("text_preprocess", sc.text_preprocess)
            doc.put("text_aggregate", sc.text_aggregate)
            doc.put("text_selected", sc.text_selected)
            doc.put("new_cluster", sc.new_cluster)
            doc.put("to_centroid", sc.to_centroid)
            doc
          }).asJava)
        })
      }
    }

    override def open(partitionId: Long, version: Long): Boolean = {
      mongoConnector = MongoConnector(writeConfig.asOptions)
      masterDataCounts = new ArrayBuffer[ColsArtifact.masterDataUpdate]()
      true
    }
  }

  val masterWord = new ForeachWriter[WrappedArray[String]] {
    var masterCollection : String = PropertiesLoader.mongoUrl + PropertiesLoader.mongoDb + "." + PropertiesLoader.dbMasterWord
    val writeConfig: WriteConfig = WriteConfig(Map("uri" -> masterCollection))
    var mongoConnector: MongoConnector = _
    var masterDataCounts: ArrayBuffer[(Int, String)] = _

    override def process(value: WrappedArray[String]): Unit = {
      value.map( row => {AggTools
        var word = row.drop(1).dropRight(1).split("\\,")
        var index = AggTools.masterWordsIndex.indexWhere(_ == word(0))
        if(index == -1){
          AggTools.masterWordsIndex += word(0)
          index = AggTools.masterWordsIndex.size - 1
        }

        masterDataCounts.append((index, word(0)))
      })
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (masterDataCounts.nonEmpty) {
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
          collection.insertMany(masterDataCounts.map(sc => {
            var doc = new Document()
            doc.put("index", sc._1)
            doc.put("word", sc._2)
            doc
          }).asJava)
        })
      }
    }

    override def open(partitionId: Long, version: Long): Boolean = {
      mongoConnector = MongoConnector(writeConfig.asOptions)
      masterDataCounts = new ArrayBuffer[(Int, String)]()
      true
    }
  }

  def saveBatchMongo(col:String, data:RDD[org.bson.Document]) = {
    var writeConfig = WriteConfig(Map("uri" -> uri, "database" -> db, "collection" -> col))
    MongoSpark.save(data, writeConfig)
  }
}
