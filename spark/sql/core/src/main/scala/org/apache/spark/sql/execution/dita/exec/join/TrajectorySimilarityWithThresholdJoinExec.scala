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

package org.apache.spark.sql.execution.dita.exec.join

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.dita.TrajectorySimilarityFunction
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.dita.algorithms.TrajectorySimilarityWithThresholdAlgorithms
import org.apache.spark.sql.execution.dita.exec.TrajectoryExecUtils
import org.apache.spark.sql.execution.dita.sql.DITAIternalRow
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}


case class TrajectorySimilarityWithThresholdJoinExec(leftKey: Expression, rightKey: Expression,
                                                     function: TrajectorySimilarityFunction,
                                                     threshold: Double,
                                                     leftLogicalPlan: LogicalPlan, rightLogicalPlan: LogicalPlan,
                                                     left: SparkPlan, right: SparkPlan) extends BinaryExecNode with Logging {
  override def output: Seq[Attribute] = left.output ++ right.output

  sparkContext.conf.registerKryoClasses(Array(classOf[Shape], classOf[Point],
    classOf[Rectangle], classOf[Trajectory]))

  protected override def doExecute(): RDD[InternalRow] = {
    logWarning(s"Distance function: $function")
    logWarning(s"Threshold: $threshold")
    val distanceFunction = TrajectorySimilarity.getDistanceFunction(function)

    val leftResults = left.execute()
    val rightResults = right.execute()
    val leftCount = leftResults.count()
    val rightCount = rightResults.count()
    logWarning(s"Data count: $leftCount, $rightCount")

    logWarning("Applying efficient trajectory similarity join algorithm!")

    val leftTrieRDD = TrajectoryExecUtils.getTrieRDD(sqlContext, leftResults,
      leftKey, leftLogicalPlan, left)
    val rightTrieRDD = TrajectoryExecUtils.getTrieRDD(sqlContext, rightResults,
      rightKey, rightLogicalPlan, right)

    // get answer
    // val join = TrajectorySimilarityWithThresholdJoinAlgorithms.SimpleDistributedJoin
    val join = TrajectorySimilarityWithThresholdAlgorithms.FineGrainedDistributedJoin
    val answerRDD = join.join(sparkContext, leftTrieRDD, rightTrieRDD,
      distanceFunction, threshold)
    val outputRDD = answerRDD.mapPartitions { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      iter.map(x =>
        joiner.join(x._1.asInstanceOf[DITAIternalRow].row,
          x._2.asInstanceOf[DITAIternalRow].row))
    }
    outputRDD.asInstanceOf[RDD[InternalRow]]
  }
}