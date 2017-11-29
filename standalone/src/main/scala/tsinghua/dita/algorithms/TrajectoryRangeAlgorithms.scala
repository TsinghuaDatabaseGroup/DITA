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

package tsinghua.dita.algorithms

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import tsinghua.dita.common.shape.Shape
import tsinghua.dita.common.trajectory.Trajectory
import tsinghua.dita.index.global.GlobalTrieIndex
import tsinghua.dita.index.local.LocalTrieIndex
import tsinghua.dita.partition.PackedPartition
import tsinghua.dita.rdd.TrieRDD

object TrajectoryRangeAlgorithms {
  def localSearch(query: Shape, packedPartition: PackedPartition, threshold: Double):
  Iterator[Trajectory] = {
    val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalTrieIndex]).head
      .asInstanceOf[LocalTrieIndex]
    val answers = localIndex.getCandidates(query, threshold)
      .filter(t => t.points.forall(p => p.minDist(query) <= threshold))
    answers.iterator
  }

  object DistributedSearch extends Logging {
    def search(sparkContext: SparkContext, query: Shape, trieRDD: TrieRDD,
               threshold: Double): RDD[Trajectory] = {
      val bQuery = sparkContext.broadcast(query)
      val globalTrieIndex = trieRDD.globalIndex.asInstanceOf[GlobalTrieIndex]

      val candidatePartitions = globalTrieIndex.getPartitions(query, threshold)
      PartitionPruningRDD.create(trieRDD.packedRDD, candidatePartitions.contains)
        .flatMap(packedPartition => localSearch(bQuery.value, packedPartition, threshold))
    }
  }
}
