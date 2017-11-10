/*
 *  Copyright 2017 by DITA Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.sql.execution.dita.partition.global

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.dita.common.ConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.execution.dita.partition.{Bounds, STRPartitioner}
import org.apache.spark.util.SizeEstimator

case class GlobalSTRPartitioner(expectedNumPartitions: Int,
                                dimension: Int,
                                maxEntriesPerNode: Int,
                                sampleRate: Double,
                                minSampleSize: Long,
                                maxSampleSize: Long,
                                dataRDD: RDD[_ <: Product2[Point, Any]])
  extends STRPartitioner(expectedNumPartitions, dimension, maxEntriesPerNode) {

  override def getBoundsAndCount: (Bounds, Long) = {
    dataRDD.aggregate[(Bounds, Long)]((null, 0))((bound, data) => {
      val new_bound = if (bound._1 == null) {
        Bounds(data._1.coord, data._1.coord)
      } else {
        Bounds(bound._1.min.zip(data._1.coord).map(x => Math.min(x._1, x._2)),
          bound._1.max.zip(data._1.coord).map(x => Math.max(x._1, x._2)))
      }
      (new_bound, bound._2 + SizeEstimator.estimate(data._1))
    }, (left, right) => {
      val new_bound = {
        if (left._1 == null) right._1
        else if (right._1 == null) left._1
        else {
          Bounds(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
            left._1.max.zip(right._1.max).map(x => Math.max(x._1, x._2)))
        }}
      (new_bound, left._2 + right._2)
    })
  }

  override def getData(totalCount: Long): Array[Point] = {
    val seed = System.currentTimeMillis()
    if (totalCount <= minSampleSize) {
      dataRDD.map(_._1).collect()
    } else if (totalCount * sampleRate <= maxSampleSize) {
      dataRDD.sample(withReplacement = false, sampleRate, seed).map(_._1).collect()
    } else {
      dataRDD.sample(withReplacement = false,
        maxSampleSize.toDouble / totalCount, seed).map(_._1).collect()
    }
  }
}

object GlobalSTRPartitioner {
  private val sampleRate = ConfigConstants.SAMPLE_RATE
  private val minSampleSize = ConfigConstants.MIN_SAMPLE_SIZE
  private val maxSampleSize = ConfigConstants.MAX_SAMPLE_SIZE
  private val maxEntriesPerNode = ConfigConstants.GLOBAL_MAX_ENTRIES_PER_NODE

  def partitionRDD(dataRDD: RDD[Point], dimension: Int, expectedNumPartitions: Int):
  (RDD[Point], GlobalSTRPartitioner) = {
    val pairedDataRDD = dataRDD.map(x => (x, None))
    val partitioner = new GlobalSTRPartitioner(expectedNumPartitions, dimension, maxEntriesPerNode,
      sampleRate, minSampleSize, maxSampleSize, pairedDataRDD)
    val shuffled = new ShuffledRDD[Point, Any, Any](pairedDataRDD, partitioner)
    (shuffled.map(_._1), partitioner)
  }

  def partitionPairedRDD(dataRDD: RDD[(Point, Array[Point])],
                         dimension: Int, expectedNumPartitions: Int):
  (RDD[(Point, Array[Point])], GlobalSTRPartitioner) = {
    val partitioner = new GlobalSTRPartitioner(expectedNumPartitions, dimension, maxEntriesPerNode,
      sampleRate, minSampleSize, maxSampleSize, dataRDD)
    val shuffled = new ShuffledRDD[Point, Array[Point], Array[Point]](dataRDD, partitioner)
    (shuffled, partitioner)
  }
}
