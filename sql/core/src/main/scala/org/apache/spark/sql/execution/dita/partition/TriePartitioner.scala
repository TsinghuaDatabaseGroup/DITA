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

package org.apache.spark.sql.execution.dita.partition

import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.expressions.dita.common.ConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.TrajectorySimilarity.{DTWDistance, EDRDistance, FrechetDistance, LCSSDistance}

abstract class TriePartitioner(partitioner: STRPartitioner,
                               childPartitioners: Array[_ <: TriePartitioner],
                               level: Int) extends Partitioner {
  val totalPartitions: Array[Int] = childPartitioners.map(_.numPartitions)
    .foldLeft(Array(0)) {case (arr, value) => arr :+ (arr.last + value)}
  def indexedPivotCount: Int = 0

  override def numPartitions: Int = if (level == 1) {
    partitioner.numPartitions
  } else {
    totalPartitions.last
  }

  override def getPartition(key: Any): Int = {
    val k = TriePartitioner.getIndexedKey(key)
    val x = partitioner.getPartition(k.head)
    if (childPartitioners.nonEmpty) {
      val y = childPartitioners(x).getPartition(k.tail)
      totalPartitions(x) + y
    } else {
      x
    }
  }

  def getPartitions(key: Any, threshold: Double,
                    distanceAccu: Double): List[(Int, Double)] = {
    val k = TriePartitioner.getSearchKey(key)
    val distanceFunction = ConfigConstants.DISTANCE_FUNCTION

    distanceFunction match {
      case DTWDistance | FrechetDistance =>
        if (level > indexedPivotCount) {
          val childPartitions = partitioner.getPartitionsWithThreshold(k.head, threshold)
          if (childPartitioners.isEmpty) {
            childPartitions.map{ case (shape, x) =>
              val distance = shape.minDist(k.head)
              val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
              (x, newDistanceAccu)
            }.filter(_._2 <= threshold)
          } else {
            childPartitions.flatMap{ case (shape, x) =>
              val distance = shape.minDist(k.head)
              val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
              val newThreshold = distanceFunction.updateThreshold(threshold, distance)
              childPartitioners(x).getPartitions(k.tail, newThreshold, newDistanceAccu)
                .map(y => (totalPartitions(x) + y._1, y._2))
            }.filter(_._2 <= threshold)
          }
        } else {
          partitioner.mbrBounds.toList.flatMap{ case (shape, x) =>
            val newK = k.dropWhile(p => shape.approxMinDist(p) > threshold)
            val distance = if (newK.isEmpty) ConfigConstants.THRESHOLD_LIMIT
            else newK.map(p => shape.minDist(p)).min
            val newDistanceAccu = distanceFunction.updateDistance(distanceAccu, distance)
            if (newDistanceAccu > threshold) {
              List.empty[(Int, Double)]
            } else {
              if (childPartitioners.isEmpty) {
                List((x, newDistanceAccu))
              } else {
                val newThreshold = distanceFunction.updateThreshold(threshold, distance)
                val ys = childPartitioners(x).getPartitions(newK,
                  newThreshold, newDistanceAccu)
                ys.map(y => (totalPartitions(x) + y._1, y._2))
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
              List.empty[(Int, Double)]
            } else {
              if (childPartitioners.isEmpty) {
                List((x, newDistanceAccu))
              } else {
                val newThreshold = distanceFunction.updateThreshold(threshold, distance)
                val ys = childPartitioners(x).getPartitions(k.tail,
                  newThreshold, newDistanceAccu)
                ys.filter(_._1 != -1).map(y => (totalPartitions(x) + y._1, y._2))
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
              List.empty[(Int, Double)]
            } else {
              if (childPartitioners.isEmpty) {
                List((x, newDistanceAccu))
              } else {
                val newThreshold = distanceFunction.updateThreshold(threshold, distance)
                val ys = childPartitioners(x).getPartitions(k,
                  newThreshold, newDistanceAccu)
                ys.filter(_._1 != -1).map(y => (totalPartitions(x) + y._1, y._2))
              }
            }
          }
        }
    }
  }
}

object TriePartitioner {
  def getIndexedKey(key: Any): Array[Point] = {
    key match {
      case t: Trajectory => t.points.head +: t.points.last +: t.getGlobalIndexedPivot
      case _ => key.asInstanceOf[Array[Point]]
    }
  }

  def getSearchKey(key: Any): Array[Point] = {
    key match {
      case t: Trajectory => t.points.head +: t.points.last +: t.points
      case _ => key.asInstanceOf[Array[Point]]
    }
  }
}
