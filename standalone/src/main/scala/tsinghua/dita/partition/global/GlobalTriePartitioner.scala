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

package tsinghua.dita.partition.global

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import tsinghua.dita.common.DITAConfigConstants
import tsinghua.dita.common.shape.Point
import tsinghua.dita.common.trajectory.Trajectory
import tsinghua.dita.partition.{STRPartitioner, TriePartitioner}

case class GlobalTriePartitioner(partitioner: STRPartitioner,
                                 childPartitioners: Array[GlobalTriePartitioner],
                                 level: Int)
  extends TriePartitioner(partitioner, childPartitioners, level) {
  override def indexedPivotCount: Int = DITAConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT

  override def getPartition(key: Any): Int = {
    val k = GlobalTriePartitioner.getIndexedKey(key)
    val x = partitioner.getPartition(k.head)
    if (childPartitioners.nonEmpty) {
      val y = childPartitioners(x).getPartition(k.tail)
      totalPartitions(x) + y
    } else {
      x
    }
  }
}

object GlobalTriePartitioner {
  def partition(dataRDD: RDD[Trajectory]): (RDD[Trajectory], GlobalTriePartitioner) = {
    // get tree partitioner
    val points = dataRDD.map(GlobalTriePartitioner.getIndexedKey)
    val totalLevels = DITAConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT + 2
    val dimension = points.take(1).head.head.coord.length
    val partitioner = partitionByLevel(points, dimension, totalLevels)

    // shuffle
    val pairedDataRDD = dataRDD.map(x => (x, None))
    val shuffled = new ShuffledRDD[Trajectory, Any, Any](pairedDataRDD, partitioner)
    (shuffled.map(_._1), partitioner)
  }

  private def getIndexedKey(key: Any): Array[Point] = {
    key match {
      case t: Trajectory => t.points.head +: t.points.last +: t.getGlobalIndexedPivot
      case _ => key.asInstanceOf[Array[Point]]
    }
  }

  private def partitionByLevel(rdd: RDD[Array[Point]],
                               dimension: Int, level: Int): GlobalTriePartitioner = {
    val numPartitions = if (level > DITAConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT) {
      DITAConfigConstants.GLOBAL_NUM_PARTITIONS
    } else {
      DITAConfigConstants.GLOBAL_PIVOT_NUM_PARTITIONS
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
}
