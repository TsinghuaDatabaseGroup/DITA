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

package org.apache.spark.sql.execution.dita.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.catalyst.expressions.dita.index.{GlobalIndex, IndexedRelation}
import org.apache.spark.sql.catalyst.expressions.dita.{PackedPartition, TrajectorySimilarityExpression}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.dita.rdd.TrieRDD

case class TrieIndexedRelation(child: SparkPlan, key: Attribute)
                              (var trieRDD: TrieRDD = null)
  extends IndexedRelation with MultiInstanceRelation {

  if (trieRDD == null) {
    trieRDD = buildIndex()
  }

  override def indexedRDD: RDD[PackedPartition] = trieRDD.packedRDD

  override def globalIndex: GlobalIndex = trieRDD.globalIndex

  override def newInstance(): IndexedRelation = {
    TrieIndexedRelation(child, key)(trieRDD).asInstanceOf[this.type]
  }

  private def buildIndex(): TrieRDD = {
    val dataRDD = child.execute().asInstanceOf[RDD[UnsafeRow]].map(row =>
      new DITAIternalRow(row, TrajectorySimilarityExpression.getPoints(
        BindReferences.bindReference(key, child.output)
          .eval(row).asInstanceOf[UnsafeArrayData]))).asInstanceOf[RDD[Trajectory]]

    new TrieRDD(dataRDD)
  }
}

