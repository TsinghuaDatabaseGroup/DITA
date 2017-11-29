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

package tsinghua.dita.index.local

import tsinghua.dita.common.shape.Shape
import tsinghua.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import tsinghua.dita.index.LocalIndex
import tsinghua.dita.partition.local.LocalTriePartitioner

case class LocalTrieIndex(partitioner: LocalTriePartitioner) extends LocalIndex {
  def getCandidates(key: Trajectory, distanceFunction: TrajectorySimilarity, threshold: Double): List[Trajectory] = {
    partitioner.getCandidates(key, distanceFunction, threshold, 0.0).filter(_._2 <= threshold).map(_._1)
  }

  def getCandidates(key: Shape, threshold: Double): List[Trajectory] = {
    partitioner.getCandidates(key, threshold)
  }

  def getCandidatesWithDistances(key: Trajectory, distanceFunction: TrajectorySimilarity, threshold: Double): List[(Trajectory, Double)] = {
    partitioner.getCandidates(key, distanceFunction, threshold, 0.0)
  }
}

object LocalTrieIndex {
  def buildIndex(data: Array[Trajectory]): LocalTrieIndex = {
    val (_, partitioner) = LocalTriePartitioner.partition(data)
    LocalTrieIndex(partitioner)
  }
}