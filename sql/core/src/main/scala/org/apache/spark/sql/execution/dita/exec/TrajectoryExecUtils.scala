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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.dita.TrajectorySimilarityExpression
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.catalyst.expressions.dita.index.IndexedRelation
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.dita.rdd.TrieRDD
import org.apache.spark.sql.execution.dita.sql.{DITAIternalRow, TrieIndexedRelation}

object TrajectoryExecUtils {
  def getIndexedRelation(sqlContext: SQLContext, plan: LogicalPlan): Option[IndexedRelation] = {
    sqlContext.sessionState.indexRegistry.lookupIndex(plan).map(_.relation)
  }

  def getTrieRDD(sqlContext: SQLContext, rdd: RDD[InternalRow], key: Expression,
                 logicalPlan: LogicalPlan,
                 physicalPlan: SparkPlan): TrieRDD = {
    getIndexedRelation(sqlContext, logicalPlan)
      .map(_.asInstanceOf[TrieIndexedRelation].trieRDD)
      .getOrElse({
        val leftRDD = rdd.asInstanceOf[RDD[UnsafeRow]].map(row =>
          new DITAIternalRow(row, TrajectorySimilarityExpression.getPoints(
            BindReferences.bindReference(key, physicalPlan.output)
              .eval(row).asInstanceOf[UnsafeArrayData]))).asInstanceOf[RDD[Trajectory]]
        new TrieRDD(leftRDD)
      })
  }
}
