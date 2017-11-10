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

import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.expressions.dita.common.ConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.execution.dita.partition.{STRPartitioner, TriePartitioner}

case class GlobalTriePartitioner(partitioner: STRPartitioner,
                                 childPartitioners: Array[GlobalTriePartitioner],
                                 level: Int)
  extends TriePartitioner(partitioner, childPartitioners, level) {


  override def indexedPivotCount: Int = ConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT
}

object GlobalTriePartitioner {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  private def partitionByLevel(rdd: RDD[Array[Point]],
                               dimension: Int, level: Int): GlobalTriePartitioner = {
    val numPartitions = if (level > ConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT) {
      ConfigConstants.GLOBAL_NUM_PARTITIONS
    } else {
      ConfigConstants.GLOBAL_PIVOT_NUM_PARTITIONS
    }

    if (level > 1) {
      val rddWithKey = rdd.map(x => (x.head, x.tail))
      val (partitionedRDD, partitioner) = GlobalSTRPartitioner.partitionPairedRDD(rddWithKey,
        dimension, numPartitions)

      val childPartitioners = (0 until partitioner.numPartitions).map(i => {
        val childRDD = partitionedRDD.mapPartitionsWithIndex(
          (idx, iter) => if (idx == i) iter.map(_._2) else Iterator())
        partitionByLevel(childRDD, dimension, level - 1)
      }).toArray

      GlobalTriePartitioner(partitioner, childPartitioners, level)
    } else {
      val rddWithoutKey = rdd.map(x => x.head)
      val (partitionedRDD, partitioner) = GlobalSTRPartitioner.partitionRDD(rddWithoutKey,
        dimension, numPartitions)

      GlobalTriePartitioner(partitioner, Array.empty, level)
    }
  }

  def partition(dataRDD: RDD[Trajectory]): (RDD[Trajectory], GlobalTriePartitioner) = {
    // get tree partitioner
    val points = dataRDD.map(TriePartitioner.getIndexedKey)
    val totalLevels = ConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT + 2
    val dimension = points.take(1).head.head.coord.length
    val partitioner = partitionByLevel(points, dimension, totalLevels)

    // shuffle
    val pairedDataRDD = dataRDD.map(x => (x, None))
    val shuffled = new ShuffledRDD[Trajectory, Any, Any](pairedDataRDD, partitioner)
    (shuffled.map(_._1), partitioner)
  }
}
