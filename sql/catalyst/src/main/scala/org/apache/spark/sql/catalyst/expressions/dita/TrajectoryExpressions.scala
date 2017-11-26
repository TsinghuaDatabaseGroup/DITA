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

package org.apache.spark.sql.catalyst.expressions.dita

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Literal, TernaryExpression, UnaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle}
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.types.{BooleanType, DataType, DataTypes, DoubleType}

case class TrajectorySimilarityExpression(function: TrajectorySimilarityFunction,
                                          traj1: Expression, traj2: Expression)
  extends BinaryExpression with CodegenFallback {

  override def left: Expression = traj1
  override def right: Expression = traj2

  override def dataType: DataType = DoubleType

  override def nullSafeEval(traj1: Any, traj2: Any): Any = function match {
    case TrajectorySimilarityFunction.DTW =>
      val trajectory1 = traj1 match {
        case t: Trajectory => t
        case uad: UnsafeArrayData => TrajectorySimilarityExpression.getTrajectory(uad)
      }
      val trajectory2 = traj2 match {
        case t: Trajectory => t
        case uad: UnsafeArrayData => TrajectorySimilarityExpression.getTrajectory(uad)
      }
      TrajectorySimilarity.DTWDistance.evalWithTrajectory(trajectory1, trajectory2)
  }
}

object TrajectorySimilarityExpression {
  def getPoints(rawData: UnsafeArrayData): Array[Point] = {
    (0 until rawData.numElements()).map(i =>
      Point(rawData.getArray(i).toDoubleArray)).toArray
  }

  def getTrajectory(rawData: UnsafeArrayData): Trajectory = {
    Trajectory((0 until rawData.numElements()).map(i =>
      Point(rawData.getArray(i).toDoubleArray)).toArray)
  }
}

case class TrajectorySimilarityWithThresholdExpression(similarity: TrajectorySimilarityExpression,
                                                       threshold: Double)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[Double] <= threshold
  }
}

case class TrajectorySimilarityWithKNNExpression(similarity: TrajectorySimilarityExpression,
                                                 count: Int)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(left: Any): Any = {
    throw new NotImplementedError()
  }
}

sealed abstract class TrajectorySimilarityFunction {
  def sql: String
}

object TrajectorySimilarityFunction {

  case object DTW extends TrajectorySimilarityFunction {
    override def sql: String = "DTW"
  }

  case object FRECHET extends TrajectorySimilarityFunction {
    override def sql: String = "FRECHET"
  }

  case object LCSS extends TrajectorySimilarityFunction {
    override def sql: String = "LCSS"
  }

  case object EDR extends TrajectorySimilarityFunction {
    override def sql: String = "EDR"
  }

  def apply(typ: String): TrajectorySimilarityFunction =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "dtw" => DTW
      case "frechet" => FRECHET
      case "lcss" => LCSS
      case "edr" => EDR
      case _ =>
        val supported = Seq("dtw")
        throw new IllegalArgumentException(s"Unsupported trajectory similarity function '$typ'. " +
          "Supported trajectory similarity functions include: "
          + supported.mkString("'", "', '", "'") + ".")
    }
}

case class TrajectoryMBRRangeExpression(traj: Expression, mbr: Rectangle)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = traj

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val trajectory = TrajectorySimilarityExpression.getTrajectory(
      input.asInstanceOf[UnsafeArrayData])
    trajectory.points.forall(mbr.contains)
  }
}

case class TrajectoryCircleRangeExpression(traj: Expression, center: Point, radius: Double)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = traj

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val trajectory = TrajectorySimilarityExpression.getTrajectory(
      input.asInstanceOf[UnsafeArrayData])
    trajectory.points.forall(p => p.minDist(center) <= radius)
  }
}