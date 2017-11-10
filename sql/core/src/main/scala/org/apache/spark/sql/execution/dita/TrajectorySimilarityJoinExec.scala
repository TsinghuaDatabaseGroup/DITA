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

package org.apache.spark.sql.execution.dita

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, Literal, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.dita.{TrajectorySimilarityExpression, TrajectorySimilarityFunction}
import org.apache.spark.sql.execution.joins.UnsafeCartesianRDD
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

case class TrajectorySimilarityJoinExec(leftKey: Expression, rightKey: Expression,
                                        function: TrajectorySimilarityFunction, threshold: Literal,
                                        left: SparkPlan, right: SparkPlan) extends BinaryExecNode {
  override def output: Seq[Attribute] = left.output ++ right.output

  protected override def doExecute(): RDD[InternalRow] = {
    val leftRDD = left.execute().map(row =>
      TrajectorySimilarityExpression.getTrajectory(BindReferences.bindReference(leftKey, left.output)
        .eval(row).asInstanceOf[UnsafeArrayData]))
    val rightRDD = right.execute().map(row =>
      TrajectorySimilarityExpression.getTrajectory(BindReferences.bindReference(leftKey, left.output)
        .eval(row).asInstanceOf[UnsafeArrayData]))

    println(s"Left Count: ${leftRDD.count()}")
    println(s"Right Count: ${rightRDD.count()}")

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]
    val spillThreshold = sqlContext.conf.cartesianProductExecBufferSpillThreshold
    val pair = new UnsafeCartesianRDD(leftResults, rightResults, right.output.size, spillThreshold)
    pair.mapPartitionsWithIndexInternal { (index, iter) =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.map { r =>
        joiner.join(r._1, r._2)
      }
    }
  }
}
