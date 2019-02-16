package com.muhalfian.spark.jobs

import com.muhalfian.spark.util._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

import scala.collection.mutable.{MutableList, ArrayBuffer, Set, HashSet, WrappedArray}

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{explode, split, col, lit, concat, udf, from_json}

import org.apache.spark.ml.linalg._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import org.apache.spark.sql.streaming.Trigger

// import org.apache.spark.ml.clustering.BisectingKMeans
// import com.muhalfian.spark.ml.BisectingKMeans

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Udaf extends StreamUtils {

  class GeometricMean extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", DoubleType) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
      StructField("count", LongType) ::
      StructField("product", DoubleType) :: Nil
    )

    // This is the output type of your aggregatation function.
    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      println(s">>> initialize (buffer: $buffer)")
      buffer(0) = 0L
      buffer(1) = 1.0
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println(s">>> update (buffer: $buffer -> input: $input)")
      buffer(0) = buffer.getAs[Long](0) + 1
      buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      println(s">>> merge (buffer1: $buffer1 -> buffer2: $buffer2)")
      buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
      buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      println(s">>> evaluate (buffer: $buffer)")
      math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
    }
  }

  def main(args: Array[String]): Unit = {

    // ===================== LOAD SPARK SESSION ============================

    val spark = getSparkSession(args)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // ====================== UDAF =========================

    spark.udf.register("gm", new GeometricMean)

    // Create a DataFrame and Spark SQL table
    import org.apache.spark.sql.functions._

    val ids = spark.range(1, 20)
    ids.registerTempTable("ids")
    val df = spark.sql("select id, id % 3 as group_id from ids")
    df.registerTempTable("simple")

    df.show()

    // Or use Dataframe syntax to call the aggregate function.

    // Create an instance of UDAF GeometricMean.
    val gm = new GeometricMean

    // Show the geometric mean of values of column "id".
    df.groupBy("group_id").agg(gm(col("id")).as("GeometricMean")).show()

    // Invoke the UDAF by its assigned name.
    df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()
  }
}
