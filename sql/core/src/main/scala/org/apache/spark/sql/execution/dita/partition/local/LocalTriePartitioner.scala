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

package org.apache.spark.sql.execution.dita.partition.local

import org.apache.spark.sql.catalyst.expressions.dita.common.ConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.TrajectorySimilarity.{DTWDistance, EDRDistance, FrechetDistance, LCSSDistance}
import org.apache.spark.sql.execution.dita.partition.{STRPartitioner, TriePartitioner}

case class LocalTriePartitioner(partitioner: STRPartitioner,
                                childPartitioners: Array[LocalTriePartitioner],
                                level: Int,
                                count: Int, data: Option[Array[List[Trajectory]]])
  extends TriePartitioner(partitioner, childPartitioners, level) {
  override def indexedPivotCount: Int = ConfigConstants.LOCAL_INDEXED_PIVOT_COUNT

  def getCandidatesWithThreshold(key: Any, threshold: Double,
                                 distanceAccu: Double): List[(Trajectory, Double)] = {
    val k = TriePartitioner.getSearchKey(key)
    val distanceFunction = ConfigConstants.DISTANCE_FUNCTION

    if (count <= ConfigConstants.LOCAL_MIN_NODE_SIZE) {
      return data.get.flatten.toList.map((_, threshold))
    }

    distanceFunction match {
      case DTWDistance | FrechetDistance =>
        if (level > indexedPivotCount) {
          val childPartitions = partitioner.getPartitionsWithThreshold(k.head, threshold)
          if (childPartitioners.isEmpty) {
            childPartitions.map{ case (shape, x) =>
              val distance = shape.minDist(k.head)
              val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
              (x, newDistanceAccu)
            }.filter(_._2 < threshold).flatMap(x => data.get(x._1).map((_, x._2)))
          } else {
            childPartitions.flatMap{ case (shape, x) =>
              val distance = shape.minDist(k.head)
              val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
              val newThreshold = distanceFunction.updateThreshold(threshold, distance)
              childPartitioners(x).getCandidatesWithThreshold(k.tail, newThreshold, newDistanceAccu)
            }
          }
        } else {
          partitioner.mbrBounds.toList.flatMap{ case (shape, x) =>
            val newK = k.dropWhile(p => shape.approxMinDist(p) > threshold)
            val distance = if (newK.isEmpty) ConfigConstants.THRESHOLD_LIMIT
            else newK.map(p => shape.approxMinDist(p)).min
            val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
            if (newDistanceAccu > threshold) {
              List.empty[(Trajectory, Double)]
            } else {
              val newThreshold = distanceFunction.updateThreshold(threshold, distance)
              if (childPartitioners.isEmpty) {
                data.get(x).map((_, newDistanceAccu))
              } else {
                childPartitioners(x).getCandidatesWithThreshold(newK, newThreshold, newDistanceAccu)
              }
            }
          }
        }
      case EDRDistance | LCSSDistance =>
        if (level > indexedPivotCount) {
          partitioner.mbrBounds.toList.flatMap{ case (shape, x) =>
            val distance = distanceFunction match {
              case LCSSDistance => LCSSDistance.subCost(shape, 0, k.head, 0)
              case EDRDistance => EDRDistance.subCost(shape, k.head)
            }
            val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
            if (newDistanceAccu > threshold) {
              List.empty[(Trajectory, Double)]
            } else {
              if (childPartitioners.isEmpty) {
                data.get(x).map((_, newDistanceAccu))
              } else {
                val newThreshold = distanceFunction.updateThreshold(threshold, distance)
                childPartitioners(x).getCandidatesWithThreshold(k.tail,
                  newThreshold, newDistanceAccu)
              }
            }
          }
        } else {
          partitioner.mbrBounds.toList.flatMap{ case (shape, x) =>
            val distance = distanceFunction match {
              case LCSSDistance =>
                if (k.map(p => shape.approxMinDist(p)).min <= LCSSDistance.EPSILON) 0 else 1
              case EDRDistance =>
                if (k.map(p => shape.approxMinDist(p)).min <= EDRDistance.EPSILON) 0 else 1
            }
            val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
            if (newDistanceAccu > threshold) {
              List.empty[(Trajectory, Double)]
            } else {
              if (childPartitioners.isEmpty) {
                data.get(x).map((_, newDistanceAccu))
              } else {
                val newThreshold = distanceFunction.updateThreshold(threshold, distance)
                childPartitioners(x).getCandidatesWithThreshold(k,
                  newThreshold, newDistanceAccu)
              }
            }
          }
        }
    }
  }
}

class EmptyLocalTriePartitioner(override val level: Int,
                                     override val count: Int,
                                     allData: List[Trajectory])
  extends LocalTriePartitioner(null, null, level, count: Int, data = None) {

  override def getCandidatesWithThreshold(key: Any, threshold: Double,
                                 distanceAccu: Double): List[(Trajectory, Double)] = {
    allData.map((_, distanceAccu))
  }
}

object LocalTriePartitioner {
  private def partitionByLevel(rdd: Array[(Array[Point], Trajectory)],
                               dimension: Int, level: Int): LocalTriePartitioner = {
    val numPartitions = if (level > ConfigConstants.LOCAL_INDEXED_PIVOT_COUNT) {
      ConfigConstants.LOCAL_NUM_PARTITIONS
    } else {
      ConfigConstants.LOCAL_PIVOT_NUM_PARTITIONS
    }

    if (rdd.length <= ConfigConstants.LOCAL_MIN_NODE_SIZE) {
      return new EmptyLocalTriePartitioner(level, rdd.length, rdd.map(_._2).toList)
    }

    if (level > 1) {
      val rddWithKey = rdd.map(x => (x._1.head, (x._1.tail, x._2)))
      // println(numPartitions)
      val (partitionedData, partitioner) = LocalSTRPartitioner.partition(rddWithKey, dimension, numPartitions)

      val childPartitioners = partitionedData.map(childRDD =>
        partitionByLevel(childRDD.map(_._2), dimension, level - 1))

      LocalTriePartitioner(partitioner, childPartitioners, level, rdd.length, None)
    } else {
      val rddWithoutKey = rdd.map(x => (x._1.head, x._2))
      val (partitionedData, partitioner) = LocalSTRPartitioner.partition(rddWithoutKey,
        dimension, numPartitions)

      LocalTriePartitioner(partitioner, Array.empty, level,
        rdd.length, Some(partitionedData.map(x => x.map(_._2).toList)))
    }
  }

  def partition(originRDD: Array[Trajectory]): (Array[Array[Trajectory]], LocalTriePartitioner) = {
    // get tree partitioner
    val points = originRDD.map(x => (TriePartitioner.getIndexedKey(x), x))
    val totalLevels = ConfigConstants.LOCAL_INDEXED_PIVOT_COUNT + 2
    val dimension = points.take(1).head._1.head.coord.length
    val partitioner = partitionByLevel(points, dimension, totalLevels)

    // shuffle
    val shuffled = originRDD.groupBy(t => partitioner.getPartition(t))
    ((0 until partitioner.numPartitions).map(i =>
      shuffled.getOrElse(i, Array.empty)).toArray, partitioner)
  }
}