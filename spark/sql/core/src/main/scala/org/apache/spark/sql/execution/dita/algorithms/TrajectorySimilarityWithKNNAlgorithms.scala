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

package org.apache.spark.sql.execution.dita.algorithms

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.dita.PackedPartition
import org.apache.spark.sql.catalyst.expressions.dita.common.DITAConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.execution.dita.index.global.GlobalTrieIndex
import org.apache.spark.sql.execution.dita.index.local.LocalTrieIndex
import org.apache.spark.sql.execution.dita.partition.global.ExactKeyPartitioner
import org.apache.spark.sql.execution.dita.rdd.TrieRDD

import scala.util.Random

object TrajectorySimilarityWithKNNAlgorithms {

  def getThresholdLocal(partitionIter: Iterator[PackedPartition],
                        trajectoryIter: Iterator[Trajectory],
                        distanceFunction: TrajectorySimilarity,
                        count: Int, initThreshold: Double): Iterator[Double] = {
    val packedPartition = partitionIter.next()
    val localIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalTrieIndex])
      .head.asInstanceOf[LocalTrieIndex]

    if (trajectoryIter.isEmpty) {
      Array.empty[Double].iterator
    } else {
      val queryTrajectories = trajectoryIter.toArray
      var finalCandidates =
        Random.shuffle(packedPartition.data.asInstanceOf[Array[Trajectory]].toList)
          .take(DITAConfigConstants.KNN_COEFFICIENT * count).toArray
          .map((queryTrajectories(Random.nextInt(queryTrajectories.length)), _, DITAConfigConstants.THRESHOLD_LIMIT))

      var threshold: Double = initThreshold
      var iteration = 1
      var canExit = false
      while (!canExit) {
        threshold = threshold / 2.0
        val candidates = queryTrajectories.flatMap(query =>
          localIndex.getCandidatesWithDistances(query, distanceFunction, threshold).map(x => (x._1, query, x._2)))
        if (candidates.length >= DITAConfigConstants.KNN_COEFFICIENT * count) {
          finalCandidates = candidates
        }
        if (candidates.length < DITAConfigConstants.KNN_COEFFICIENT * count
          || iteration > DITAConfigConstants.KNN_MAX_LOCAL_ITERATION) {
          canExit = true
        }
        iteration += 1
      }

      finalCandidates.sortBy(_._3).take(DITAConfigConstants.KNN_COEFFICIENT * count)
        .map(x => distanceFunction.evalWithTrajectory(x._1, x._2, initThreshold))
        .sorted.take(count).iterator
    }
  }

  object DistributedSearch extends Logging {
    implicit val order = new Ordering[(Trajectory, Double)] {
      def compare(x: (Trajectory, Double), y: (Trajectory, Double)): Int = {
        x._2.compare(y._2)
      }
    }

    def search(sparkContext: SparkContext, query: Trajectory, trieRDD: TrieRDD,
               distanceFunction: TrajectorySimilarity,
               count: Int): RDD[(Trajectory, Double)] = {
      val threshold = trieRDD.packedRDD.mapPartitions(iter =>
        getThresholdLocal(iter, Iterator(query), distanceFunction, count, Double.MaxValue))
        .collect().sorted.take(count).last
      logWarning(s"Threshold: $threshold")

      val answerRDD = TrajectorySimilarityWithThresholdAlgorithms.DistributedSearch.search(
        sparkContext, query, trieRDD, distanceFunction, threshold)
      logWarning(s"Answer Count: ${answerRDD.count()}")

      sparkContext.parallelize(answerRDD.takeOrdered(count))
    }
  }

  object DistributedJoin extends Logging {
    implicit val order = new Ordering[(Trajectory, Trajectory, Double)] {
      def compare(x: (Trajectory, Trajectory, Double), y: (Trajectory, Trajectory, Double)): Int = {
        x._3.compare(y._3)
      }
    }

    def join(sparkContext: SparkContext, leftTrieRDD: TrieRDD, rightTrieRDD: TrieRDD,
             distanceFunction: TrajectorySimilarity,
             count: Int): RDD[(Trajectory, Trajectory, Double)] = {
      val threshold = getThreshold(sparkContext, leftTrieRDD, rightTrieRDD, distanceFunction, count)
      logWarning(s"Threshold: $threshold")
      val answerRDD = TrajectorySimilarityWithThresholdAlgorithms.SimpleDistributedJoin
        .join(sparkContext, leftTrieRDD, rightTrieRDD, distanceFunction, threshold)
      logWarning(s"Answer Count: ${answerRDD.count()}")
      sparkContext.parallelize(answerRDD.takeOrdered(count))
    }

    private def getThreshold(sparkContext: SparkContext,
                             leftTrieRDD: TrieRDD, rightTrieRDD: TrieRDD,
                             distanceFunction: TrajectorySimilarity,
                             count: Int): Double = {
      // basic variables
      val globalTrieIndex = leftTrieRDD.globalIndex.asInstanceOf[GlobalTrieIndex]
      val bGlobalTrieIndex = sparkContext.broadcast(globalTrieIndex)
      val leftNumPartitions = leftTrieRDD.packedRDD.partitions.length

      // get threshold
      val rightSingleCandidatesRDD = rightTrieRDD.packedRDD.flatMap(packedPartition =>
        packedPartition.getSample(DITAConfigConstants.KNN_MAX_SAMPLING_RATE)
          .asInstanceOf[List[Trajectory]].map(trajectory =>
          (bGlobalTrieIndex.value.getPartitions(trajectory, distanceFunction, 0.0).head, trajectory)
        )
      )
      val partitionedRightSingleCandidatesRDD = ExactKeyPartitioner.partition(
        rightSingleCandidatesRDD, leftNumPartitions)

      var finalThreshold = 1.0
      var sampleRate = math.pow(10, -(DITAConfigConstants.KNN_MAX_GLOBAL_ITERATION - 1))
      for {_ <- 1 to DITAConfigConstants.KNN_MAX_GLOBAL_ITERATION} {
        val allThresholds = leftTrieRDD.packedRDD.zipPartitions(
          partitionedRightSingleCandidatesRDD.sample(true, sampleRate)) { case (partitionIter, trajectoryIter) =>
          getThresholdLocal(partitionIter, trajectoryIter, distanceFunction, count, finalThreshold)
        }.collect().sorted.take(count)
        if (allThresholds.nonEmpty) {
          finalThreshold = math.min(finalThreshold, allThresholds.last)
        }
        sampleRate *= 10
      }

      finalThreshold
    }
  }

}