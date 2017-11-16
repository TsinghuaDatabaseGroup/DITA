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

package org.apache.spark.sql.catalyst.expressions.dita.index

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.catalyst.expressions.dita.{PackedPartition, TrajectorySimilarityExpression}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, UnsafeArrayData}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class IndexedRelation extends LogicalPlan {
  def indexedRDD: RDD[PackedPartition]
  def globalIndex: GlobalIndex

  override def output: Seq[Attribute] = Nil

  override def children: Seq[LogicalPlan] = Nil
}
