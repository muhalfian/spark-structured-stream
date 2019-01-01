/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apache.spark.ml.clustering

import org.apache.spark.ml.clustering.BisectingKMeans
import com.apache.spark.ml.util.CustomDatasetUtils

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.clustering.{BisectingKMeans => MLlibBisectingKMeans,
  BisectingKMeansModel => MLlibBisectingKMeansModel}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StructType}


@Since("2.0.0")
class CustomBisectingKMeans extends BisectingKMeans {

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): BisectingKMeansModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)
    val rdd = CustomDatasetUtils.columnToOldVector(dataset, getFeaturesCol)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, featuresCol, predictionCol, k, maxIter, seed,
      minDivisibleClusterSize, distanceMeasure)

    val bkm = new MLlibBisectingKMeans()
      .setK($(k))
      .setMaxIterations($(maxIter))
      .setMinDivisibleClusterSize($(minDivisibleClusterSize))
      .setSeed($(seed))
      .setDistanceMeasure($(distanceMeasure))
    val parentModel = bkm.run(rdd, Some(instr))
    val model = copyValues(new BisectingKMeansModel(uid, parentModel).setParent(this))
    val summary = new BisectingKMeansSummary(
      model.transform(dataset), $(predictionCol), $(featuresCol), $(k), $(maxIter))
    instr.logNamedValue("clusterSizes", summary.clusterSizes)
    instr.logNumFeatures(model.clusterCenters.head.size)
    model.setSummary(Some(summary))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}
