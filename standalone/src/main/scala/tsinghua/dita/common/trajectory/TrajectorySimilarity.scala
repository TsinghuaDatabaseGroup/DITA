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

package tsinghua.dita.common.trajectory

import tsinghua.dita.common.DITAConfigConstants
import tsinghua.dita.common.shape.{Point, Rectangle, Shape}

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

  object DTWDistance extends TrajectorySimilarity {
    private final val MAX_COST = Array.fill[Double](1, 1)(DITAConfigConstants.THRESHOLD_LIMIT)

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
      val cellDistance = if (t1.getCells.isEmpty || t2.getCells.isEmpty) {
        0.0
      } else {
        math.max(cellEstimation(t1, t2, threshold), cellEstimation(t2, t1, threshold))
      }

      if (cellDistance > threshold) {
        cellDistance
      } else {
        val costVector = getDistanceVector(t1.points, t2.points, threshold)
        costVector.last.last
      }
    }

    private def cellEstimation(t1: Trajectory, t2: Trajectory, threshold: Double): Double = {
      val cost = Array.ofDim[Double](t1.getCells.length, t2.getCells.length)
      var totalDistance = 0.0
      for (index <- t2.getCells.indices) {
        if (totalDistance <= threshold) {
          var distance = DITAConfigConstants.THRESHOLD_LIMIT
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
      val cost = Array.fill[Double](points1.length, points2.length)(DITAConfigConstants.THRESHOLD_LIMIT)

      for (i <- points1.indices) {
        for (j <- points2.indices) {
          cost(i)(j) = points1(i).minDist(points2(j))
          val left = if (i > 0) cost(i - 1)(j) else DITAConfigConstants.THRESHOLD_LIMIT
          val up = if (j > 0) cost(i)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
          val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
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
        var minDist = DITAConfigConstants.THRESHOLD_LIMIT
        for (j <- points2.indices) {
          if (i > 0 || j > 0) {
            val left = if (i > 0) cost(i - 1)(j) else DITAConfigConstants.THRESHOLD_LIMIT
            val up = if (j > 0) cost(i)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
            val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
            val last = math.min(math.min(left, up), diag)
            if (last > threshold) {
              cost(i)(j) = DITAConfigConstants.THRESHOLD_LIMIT
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

  object FrechetDistance extends TrajectorySimilarity {
    private final val MAX_COST = Array.fill[Double](1, 1)(DITAConfigConstants.THRESHOLD_LIMIT)

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
      val costVector = getDistanceVector(t1.points, t2.points, threshold)
      costVector.last.last

      val cellDistance = if (t1.getCells.isEmpty || t2.getCells.isEmpty) {
        0.0
      } else {
        evalWithTrajectory(t1.getCells.map(_._1), t2.getCells.map(_._1), threshold)
      }

      if (cellDistance > threshold) {
        cellDistance
      } else {
        val costVector = getDistanceVector(t1.points, t2.points)
        costVector.last.last
      }
    }

    private def getDistanceVector[T <: Shape : ClassTag]
    (points1: Array[T], points2: Array[T]): Array[Array[Double]] = {
      val cost = Array.fill[Double](points1.length, points2.length)(DITAConfigConstants.THRESHOLD_LIMIT)

      for (i <- points1.indices) {
        for (j <- points2.indices) {
          cost(i)(j) = points1(i).minDist(points2(j))
          val left = if (i > 0) cost(i - 1)(j) else DITAConfigConstants.THRESHOLD_LIMIT
          val up = if (j > 0) cost(i)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
          val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
          val last = math.min(math.min(left, up), diag)
          if (i > 0 || j > 0) {
            cost(i)(j) = math.max(cost(i)(j), last)
          }
        }
      }

      cost
    }

    private def getDistanceVector[T <: Shape : ClassTag]
    (points1: Array[T], points2: Array[T], threshold: Double): Array[Array[Double]] = {
      val cost = Array.ofDim[Double](points1.length, points2.length)

      for (i <- points1.indices) {
        var minDist = DITAConfigConstants.THRESHOLD_LIMIT
        for (j <- points2.indices) {
          if (i > 0 || j > 0) {
            val left = if (i > 0) cost(i - 1)(j) else DITAConfigConstants.THRESHOLD_LIMIT
            val up = if (j > 0) cost(i)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
            val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) else DITAConfigConstants.THRESHOLD_LIMIT
            val last = math.min(math.min(left, up), diag)
            if (last > threshold) {
              cost(i)(j) = DITAConfigConstants.THRESHOLD_LIMIT
            } else {
              cost(i)(j) = math.max(points1(i).minDist(points2(j)), last)
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
      threshold
    }

    override def updateDistance(distanceAccu: Double, newDistance: Double): Double = {
      math.max(distanceAccu, newDistance)
    }
  }

  object EDRDistance extends TrajectorySimilarity {
    private final val MAX_COST = Array.fill[Double](1, 1)(DITAConfigConstants.THRESHOLD_LIMIT)
    var EPSILON = 0.0001

    override def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle]): Double = {
      throw new NotImplementedError
    }

    override def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle],
                           threshold: Double): Double = {
      throw new NotImplementedError
    }

    override def evalWithTrajectory(t1: Trajectory, t2: Trajectory): Double = {
      throw new NotImplementedError
    }

    override def evalWithTrajectory(t1: Trajectory, t2: Trajectory, threshold: Double): Double = {
      val lengthDiff = math.abs(t1.points.length - t2.points.length)
      if (lengthDiff > threshold) {
        lengthDiff.toDouble
      } else {
        val costVector = getDistanceVector(t1.points, t2.points, threshold)
        costVector.last.last
      }
    }

    def subCost(shape1: Shape, shape2: Shape): Double = {
      if (shape1.minDist(shape2) <= EPSILON) {
        0
      } else {
        1
      }
    }

    private def getDistanceVector[T <: Shape : ClassTag]
    (points1: Array[T], points2: Array[T], threshold: Double): Array[Array[Double]] = {
      val cost = Array.ofDim[Double](points1.length, points2.length)

      for (i <- points1.indices) {
        var minDist = DITAConfigConstants.THRESHOLD_LIMIT
        for (j <- points2.indices) {
          if (i > 0 || j > 0) {
            val left = if (i > 0) cost(i - 1)(j) + 1 else DITAConfigConstants.THRESHOLD_LIMIT
            val up = if (j > 0) cost(i)(j - 1) + 1 else DITAConfigConstants.THRESHOLD_LIMIT
            val diag = if (i > 0 && j > 0) cost(i - 1)(j - 1) + subCost(points1(i), points2(j))
            else DITAConfigConstants.THRESHOLD_LIMIT
            val last = math.min(math.min(left, up), diag)
            if (last > threshold) {
              cost(i)(j) = DITAConfigConstants.THRESHOLD_LIMIT
            } else {
              cost(i)(j) = last
            }
          } else {
            cost(i)(j) = subCost(points1(i), points2(j))
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


  object LCSSDistance extends TrajectorySimilarity {
    private final val MAX_COST = Array.fill[Double](1, 1)(DITAConfigConstants.THRESHOLD_LIMIT)
    var EPSILON = 0.0001
    var DELTA = 3

    override def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle]): Double = {
      throw new NotImplementedError
    }

    def evalWithTrajectory(t1: Array[Rectangle], t2: Array[Rectangle],
                           threshold: Double): Double = {
      throw new NotImplementedError
    }

    override def evalWithTrajectory(t1: Trajectory, t2: Trajectory): Double = {
      throw new NotImplementedError
    }

    override def evalWithTrajectory(t1: Trajectory, t2: Trajectory, threshold: Double): Double = {
      val lengthDiff = math.abs(t1.points.length - t2.points.length)
      if (lengthDiff > threshold) {
        lengthDiff.toDouble
      } else {
        val costVector = getDistanceVector(t1.points, t2.points, threshold)
        costVector.last.last
      }
    }

    def subCost(shape1: Shape, index1: Int, shape2: Shape, index2: Int): Double = {
      if (math.abs(index1 - index2) <= DELTA && shape1.minDist(shape2) <= EPSILON) {
        0
      } else {
        1
      }
    }

    private def getDistanceVector[T <: Shape : ClassTag]
    (points1: Array[T], points2: Array[T], threshold: Double): Array[Array[Double]] = {
      val cost = Array.ofDim[Double](points1.length, points2.length)

      for (i <- points1.indices) {
        var minDist = DITAConfigConstants.THRESHOLD_LIMIT
        for (j <- points2.indices) {
          if (i > 0 || j > 0) {
            val left = if (i > 0) cost(i - 1)(j) + 1 else DITAConfigConstants.THRESHOLD_LIMIT
            val up = if (j > 0) cost(i)(j - 1) + 1 else DITAConfigConstants.THRESHOLD_LIMIT
            val diag = if (i > 0 && j > 0) {
              cost(i - 1)(j - 1) + subCost(points1(i), i, points2(j), j)
            } else DITAConfigConstants.THRESHOLD_LIMIT
            val last = math.min(math.min(left, up), diag)
            if (last > threshold) {
              cost(i)(j) = DITAConfigConstants.THRESHOLD_LIMIT
            } else {
              cost(i)(j) = last
            }
          } else {
            cost(i)(j) = subCost(points1(i), i, points2(j), j)
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
