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
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.execution.dita.index.local.RTree

import scala.collection.mutable

case class Bounds(min: Array[Double], max: Array[Double])

abstract class STRPartitioner(expectedNumPartitions: Int, dimension: Int,
                              maxEntriesPerNode: Int) extends Partitioner {

  var mbrBounds: Array[(Rectangle, Int)] = {
    val (dataBounds, totalCount) = getBoundsAndCount
    val data = getData(totalCount)

    val mbrs = if (expectedNumPartitions > 1) {
      val dimensionCount = new Array[Int](dimension)
      var remaining = expectedNumPartitions.toDouble
      for (i <- 0 until dimension) {
        dimensionCount(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dimensionCount(i)
      }

      val currentBounds = Bounds(new Array[Double](dimension), new Array[Double](dimension))
      recursiveGroupPoint(dimensionCount, dataBounds, data, currentBounds, 0, dimension - 1)
    } else {
      if (dataBounds == null) {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        Array(Rectangle(Point(min), Point(max)))
      } else {
        Array(Rectangle(Point(dataBounds.min), Point(dataBounds.max)))
      }
    }

    mbrs.zipWithIndex
  }

  val rTree: RTree = if (mbrBounds.nonEmpty) {
    RTree(mbrBounds.map(x => (x._1, x._2, 1)), maxEntriesPerNode)
  } else {
    null
  }

  override def numPartitions: Int = mbrBounds.length

  override def getPartition(key: Any): Int = {
    if (rTree == null) {
      -1
    } else {
      val k = key.asInstanceOf[Point]
      val partitions = rTree.circleRange(k, 0.0)
      partitions((k.toString.hashCode % partitions.length +
        partitions.length) % partitions.length)._2
    }
  }

  def getPartitionsWithThreshold(key: Any, threshold: Double): List[(Shape, Int)] = {
    if (rTree == null) {
      List.empty
    } else {
      val k = key.asInstanceOf[Point]
      rTree.circleRange(k, threshold)
    }
  }

  def getBoundsAndCount: (Bounds, Long)

  def getData(totalCount: Long): Array[Point]

  private def recursiveGroupPoint(dimensionCount: Array[Int], dataBounds: Bounds,
                                  entries: Array[Point], currentBounds: Bounds,
                                  currentDimension: Int, untilDimension: Int): Array[Rectangle] = {
    var ans = mutable.ArrayBuffer[Rectangle]()
    if (entries.isEmpty) {
      return ans.toArray
    }

    val len = entries.length.toDouble
    val grouped = entries.sortWith(_.coord(currentDimension) < _.coord(currentDimension))
      .grouped(Math.ceil(len / dimensionCount(currentDimension)).toInt).toArray
    if (currentDimension < untilDimension) {
      for (i <- grouped.indices) {
        if (i == 0 && i == grouped.length - 1) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else if (i == 0) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord(currentDimension)
        } else if (i == grouped.length - 1) {
          currentBounds.min(currentDimension) = grouped(i).head.coord(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else {
          currentBounds.min(currentDimension) = grouped(i).head.coord(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord(currentDimension)
        }
        ans ++= recursiveGroupPoint(dimensionCount, dataBounds, grouped(i),
          currentBounds, currentDimension + 1, untilDimension)
      }
      ans.toArray
    } else {
      for (i <- grouped.indices) {
        if (i == 0 && i == grouped.length - 1) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else if (i == 0) {
          currentBounds.min(currentDimension) = dataBounds.min(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord(currentDimension)
        } else if (i == grouped.length - 1) {
          currentBounds.min(currentDimension) = grouped(i).head.coord(currentDimension)
          currentBounds.max(currentDimension) = dataBounds.max(currentDimension)
        } else {
          currentBounds.min(currentDimension) = grouped(i).head.coord(currentDimension)
          currentBounds.max(currentDimension) = grouped(i + 1).head.coord(currentDimension)
        }
        ans += Rectangle(Point(currentBounds.min.clone()), Point(currentBounds.max.clone()))
      }
      ans.toArray
    }
  }
}
