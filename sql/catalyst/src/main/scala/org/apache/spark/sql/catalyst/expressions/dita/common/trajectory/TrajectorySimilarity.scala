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

package org.apache.spark.sql.catalyst.expressions.dita.common.trajectory

import org.apache.spark.sql.catalyst.expressions.dita.common.ConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}

import scala.reflect.ClassTag

object PointDistance {
  def eval(v: Point): Double = {
    math.sqrt(v.coord.map(x => x * x).sum)
  }
}

trait TrajectorySimilarity extends Serializable {
  def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle]): Double

  def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle], threshold: Double): Double

  def evalWithTrajectory(t1: Trajectory, t2: Trajectory): Double

  def evalWithTrajectory(t1: Trajectory, t2: Trajectory, threshold: Double): Double

  def updateThreshold(threshold: Double, distance: Double): Double

  def updateDistance(distanceAccu: Double, newDistance: Double): Double
}

object TrajectorySimilarity {

  object DTW extends TrajectorySimilarity {
    private final val MAX_COST = Array.fill[Double](1, 1)(ConfigConstants.THRESHOLD_LIMIT)

    override def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle]): Double = {
      val costVector = getDistanceVector(t1, t2)
      costVector.last.last
    }

    def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle],
                           threshold: Double): Double = {
      val costVector = getDistanceVector(t1, t2, threshold)
      costVector.last.last
    }

    override def evalWithTrajectory(t1: Trajectory, t2: Trajectory): Double = {
      val costVector = getDistanceVector(t1.points, t2.points)
      costVector.last.last
    }

    override def evalWithTrajectory(t1: Trajectory, t2: Trajectory, threshold: Double): Double = {
      val cellDistance = cellEstimation(t1, t2, threshold)
      if (cellDistance > threshold) {
        0.0
      } else {
        evalWithTrajectory(t1, t2, threshold)
      }
    }

    private def cellEstimation(t1: Trajectory, t2: Trajectory, threshold: Double): Double = {
      val cost = Array.ofDim[Double](t1.getCells.length, t2.getCells.length)
      var totalDistance = 0.0
      for (index <- t2.getCells.indices) {
        if (totalDistance <= threshold) {
          var distance = ConfigConstants.THRESHOLD_LIMIT
          t1.getCells.zipWithIndex.foreach(cell => {
            cost(cell._2)(index) = cell._1._1.approxMinDist(t2.getCells(index)._1)
            distance = math.min(distance, cost(cell._2)(index))
          })
          totalDistance += distance
        }
      }
      if (totalDistance > threshold) {
        return totalDistance
      }

      t1.getCells.zipWithIndex.map(cell => cost(cell._2).min * cell._1._2).sum
    }

    private def getDistanceVector[T <: Shape : ClassTag]
    (points1: Array[T], points2: Array[T]): Array[Array[Double]] = {
      val cost = Array.fill[Double](points1.length, points2.length)(ConfigConstants.THRESHOLD_LIMIT)

      for (i <- points1.indices) {
        for (j <- points2.indices) {
          cost(i)(j) = points1(i).minDist(points2(j))
          val left = if (i > 0) cost(i - 1)(j) else ConfigConstants.THRESHOLD_LIMIT
          val up = if (j > 0) cost(i)(j - 1) else ConfigConstants.THRESHOLD_LIMIT
          val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) else ConfigConstants.THRESHOLD_LIMIT
          val last = math.min(math.min(left, up), diag)
          if (i > 0 || j > 0) {
            cost(i)(j) += last
          }
        }
      }

      cost
    }

    private def getDistanceVector[T <: Shape : ClassTag]
    (points1: Array[T], points2: Array[T], threshold: Double): Array[Array[Double]] = {
      val cost = Array.ofDim[Double](points1.length, points2.length)

      for (i <- points1.indices) {
        var minDist = ConfigConstants.THRESHOLD_LIMIT
        for (j <- points2.indices) {
          if (i > 0 || j > 0) {
            val left = if (i > 0) cost(i - 1)(j) else ConfigConstants.THRESHOLD_LIMIT
            val up = if (j > 0) cost(i)(j - 1) else ConfigConstants.THRESHOLD_LIMIT
            val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) else ConfigConstants.THRESHOLD_LIMIT
            val last = math.min(math.min(left, up), diag)
            if (last > threshold) {
              cost(i)(j) = ConfigConstants.THRESHOLD_LIMIT
            } else {
              cost(i)(j) = points1(i).minDist(points2(j)) + last
            }
          } else {
            cost(i)(j) = points1(i).minDist(points2(j))
          }
          minDist = math.min(minDist, cost(i)(j))
        }

        if (minDist > threshold) {
          return MAX_COST
        }
      }

      cost
    }

    override def updateThreshold(threshold: Double, distance: Double): Double = {
      threshold - distance
    }

    override def updateDistance(distanceAccu: Double, newDistance: Double): Double = {
      distanceAccu + newDistance
    }
  }

}
