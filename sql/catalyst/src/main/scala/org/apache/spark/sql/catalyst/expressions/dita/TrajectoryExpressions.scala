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

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

case class TrajectorySimilarityExpression(function: TrajectorySimilarityFunction,
                                          traj1: Expression, traj2: Expression)
  extends BinaryExpression with CodegenFallback {

  override def left: Expression = traj1
  override def right: Expression = traj2

  override def dataType: DataType = DoubleType

  override def nullSafeEval(traj1: Any, traj2: Any): Any = function match {
    case DTW =>
      val trajectory1 = TrajectorySimilarityExpression.getTrajectory(traj1.asInstanceOf[UnsafeArrayData])
      val trajectory2 = TrajectorySimilarityExpression.getTrajectory(traj2.asInstanceOf[UnsafeArrayData])
      TrajectorySimilarity.DTWDistance.evalWithTrajectory(trajectory1, trajectory2)
  }
}

object TrajectorySimilarityExpression {
  def getTrajectory(rawData: UnsafeArrayData): Trajectory = {
    Trajectory((0 until rawData.numElements()).map(i => Point(rawData.getArray(i).toDoubleArray)).toArray)
  }
}

object TrajectorySimilarityFunction {
  def apply(typ: String): TrajectorySimilarityFunction =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "dtw" => DTW
      case _ =>
        val supported = Seq("dtw")
        throw new IllegalArgumentException(s"Unsupported trajectory similarity function '$typ'. " +
          "Supported trajectory similarity functions include: "
          + supported.mkString("'", "', '", "'") + ".")
    }
}

sealed abstract class TrajectorySimilarityFunction {
  def sql: String
}

case object DTW extends TrajectorySimilarityFunction {
  override def sql: String = "DTW"
}