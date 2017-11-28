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

package org.apache.spark.sql.execution.dita.exec.search

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.dita.TrajectorySimilarityFunction
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.dita.algorithms.TrajectorySimilarityWithKNNAlgorithms
import org.apache.spark.sql.execution.dita.exec.TrajectoryExecUtils
import org.apache.spark.sql.execution.dita.sql.DITAIternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class TrajectorySimilarityWithKNNSearchExec(leftQuery: Trajectory, rightKey: Expression,
                                                 function: TrajectorySimilarityFunction,
                                                 count: Int,
                                                 rightLogicalPlan: LogicalPlan,
                                                 right: SparkPlan)
  extends UnaryExecNode with Logging {

  override def output: Seq[Attribute] = right.output

  override def child: SparkPlan = right

  sparkContext.conf.registerKryoClasses(Array(classOf[Shape], classOf[Point],
    classOf[Rectangle], classOf[Trajectory]))

  protected override def doExecute(): RDD[InternalRow] = {
    val distanceFunction = TrajectorySimilarity.getDistanceFunction(function)
    logWarning(s"Distance function: $function")
    logWarning(s"Count: $count")

    val rightResults = right.execute()
    val rightCount = rightResults.count()
    logWarning(s"Data count: $rightCount")

    logWarning("Applying efficient trajectory similarity search algorithm!")

    val rightTrieRDD = TrajectoryExecUtils.getTrieRDD(sqlContext, rightResults,
      rightKey, rightLogicalPlan, right)

    // get answer
    val search = TrajectorySimilarityWithKNNAlgorithms.DistributedSearch
    val answerRDD = search.search(sparkContext, leftQuery,
      rightTrieRDD, distanceFunction, count)
    val outputRDD = answerRDD.map(x => x._1.asInstanceOf[DITAIternalRow].row)
    outputRDD.asInstanceOf[RDD[InternalRow]]
  }
}
