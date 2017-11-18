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

package org.apache.spark.sql.execution.dita.exec

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.catalyst.expressions.dita.index.IndexedRelation
import org.apache.spark.sql.catalyst.expressions.dita.{TrajectorySimilarityExpression, TrajectorySimilarityFunction}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.dita.rdd.TrieRDD
import org.apache.spark.sql.execution.dita.sql.{DITAIternalRow, TrieIndexedRelation}
import org.apache.spark.sql.execution.joins.UnsafeCartesianRDD
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}


case class TrajectorySimilarityWithThresholdJoinExec(leftKey: Expression, rightKey: Expression,
                                                     function: TrajectorySimilarityFunction,
                                                     threshold: Double,
                                                     leftLogicalPlan: LogicalPlan, rightLogicalPlan: LogicalPlan,
                                                     left: SparkPlan, right: SparkPlan) extends BinaryExecNode with Logging {
  override def output: Seq[Attribute] = left.output ++ right.output

  val MIN_OPTIMIZATION_COUNT: Long = 1000

  sparkContext.conf.registerKryoClasses(Array(classOf[Shape], classOf[Point],
    classOf[Rectangle], classOf[Trajectory]))

  protected override def doExecute(): RDD[InternalRow] = {
    logWarning(s"Distance function: $function")
    logWarning(s"Threshold: $threshold")

    val leftResults = left.execute()
    val rightResults = right.execute()
    val leftCount = leftResults.count()
    val rightCount = rightResults.count()
    logWarning(s"Data count: $leftCount, $rightCount")

    if (leftCount > MIN_OPTIMIZATION_COUNT && rightCount > MIN_OPTIMIZATION_COUNT) {
      logWarning("Applying efficient trajectory similarity join algorithm!")

      val distanceFunction = TrajectorySimilarity.getDistanceFunction(function)

      val leftTrieRDD = TrajectorySimilarityWithThresholdJoinExec
        .getIndexedRelation(sqlContext, leftLogicalPlan)
        .map(_.asInstanceOf[TrieIndexedRelation].trieRDD)
        .getOrElse({
          logWarning("Building left trie RDD")
          val leftRDD = leftResults.map(row =>
            new DITAIternalRow(row, TrajectorySimilarityExpression.getPoints(
              BindReferences.bindReference(leftKey, left.output)
                .eval(row).asInstanceOf[UnsafeArrayData]))).asInstanceOf[RDD[Trajectory]]
          new TrieRDD(leftRDD)
        })

      val rightTrieRDD = TrajectorySimilarityWithThresholdJoinExec
        .getIndexedRelation(sqlContext, rightLogicalPlan)
        .map(_.asInstanceOf[TrieIndexedRelation].trieRDD)
        .getOrElse({
          logWarning("Building right trie RDD")
          val rightRDD = rightResults.map(row =>
            new DITAIternalRow(row, TrajectorySimilarityExpression.getPoints(
              BindReferences.bindReference(rightKey, right.output)
                .eval(row).asInstanceOf[UnsafeArrayData]))).asInstanceOf[RDD[Trajectory]]
          new TrieRDD(rightRDD)
        })

      // get answer
      // val join = TrajectorySimilarityWithThresholdJoinAlgorithms.SimpleDistributedJoin
      val join = TrajectorySimilarityWithThresholdJoinAlgorithms.FineGrainedDistributedJoin
      val answerRDD = join.join(sparkContext, leftTrieRDD, rightTrieRDD,
        distanceFunction, threshold)
      val outputRDD = answerRDD.mapPartitions { iter =>
        iter.map(x =>
          new JoinedRow(x._1.asInstanceOf[DITAIternalRow].row,
            x._2.asInstanceOf[DITAIternalRow].row))
      }
      outputRDD.asInstanceOf[RDD[InternalRow]]
    } else {
      val spillThreshold = sqlContext.conf.cartesianProductExecBufferSpillThreshold
      val pair = new UnsafeCartesianRDD(leftResults.asInstanceOf[RDD[UnsafeRow]],
        rightResults.asInstanceOf[RDD[UnsafeRow]], right.output.size, spillThreshold)
      pair.mapPartitionsWithIndexInternal { (index, iter) =>
        val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
        iter.map { r =>
          joiner.join(r._1, r._2)
        }
      }
    }
  }
}

object TrajectorySimilarityWithThresholdJoinExec {
  def getIndexedRelation(sqlContext: SQLContext, plan: LogicalPlan): Option[IndexedRelation] = {
    sqlContext.sessionState.indexRegistry.lookupIndex(plan).map(_.relation)
  }
}