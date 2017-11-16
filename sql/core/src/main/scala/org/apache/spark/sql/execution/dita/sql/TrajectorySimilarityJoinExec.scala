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

import org.apache.spark.Accumulable
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.dita.common.DITAConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import org.apache.spark.sql.catalyst.expressions.dita.index.IndexedRelation
import org.apache.spark.sql.catalyst.expressions.dita.{PackedPartition, TrajectorySimilarityExpression, TrajectorySimilarityFunction}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, Literal, UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.dita.index.global.GlobalTrieIndex
import org.apache.spark.sql.execution.dita.index.local.LocalTrieIndex
import org.apache.spark.sql.execution.dita.partition.global.ExactKeyPartitioner
import org.apache.spark.sql.execution.dita.rdd.TrieRDD
import org.apache.spark.sql.execution.dita.util.HashMapParam
import org.apache.spark.sql.execution.joins.UnsafeCartesianRDD
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

import scala.collection.mutable.{HashMap, HashSet}
import scala.util.control.Breaks


case class TrajectorySimilarityJoinExec(leftKey: Expression, rightKey: Expression,
                                        function: TrajectorySimilarityFunction,
                                        thresholdLiteral: Literal,
                                        leftLogicalPlan: LogicalPlan, rightLogicalPlan: LogicalPlan,
                                        left: SparkPlan, right: SparkPlan) extends BinaryExecNode with Logging {
  override def output: Seq[Attribute] = left.output ++ right.output

  val LAMBDA: Double = 2.0
  val MIN_OPTIMIZATION_COUNT: Long = 1000

  sparkContext.conf.registerKryoClasses(Array(classOf[Shape], classOf[Point],
    classOf[Rectangle], classOf[Trajectory]))

  protected override def doExecute(): RDD[InternalRow] = {
    logWarning(s"Distance function: $function")
    val threshold = thresholdLiteral.value.asInstanceOf[Number].doubleValue()
    logWarning(s"Threshold: $threshold")

    val leftResults = left.execute()
    val rightResults = right.execute()
    val leftCount = leftResults.count()
    val rightCount = rightResults.count()
    logWarning(s"Data count: $leftCount, $rightCount")

    if (leftCount > MIN_OPTIMIZATION_COUNT && rightCount > MIN_OPTIMIZATION_COUNT) {
      logWarning("Applying efficient trajectory similarity join algorithm!")

      val distanceFunction = TrajectorySimilarity.getDistanceFunction(function)

      val leftTrieRDD = getIndexedRelation(leftLogicalPlan)
        .map(_.asInstanceOf[TrieIndexedRelation].trieRDD)
        .getOrElse({
          logWarning("Building left trie RDD")
          val leftRDD = leftResults.map(row =>
            new DITAIternalRow(row, TrajectorySimilarityExpression.getPoints(
              BindReferences.bindReference(leftKey, left.output)
                .eval(row).asInstanceOf[UnsafeArrayData]))).asInstanceOf[RDD[Trajectory]]
          new TrieRDD(leftRDD)
        })

      val rightTrieRDD = getIndexedRelation(rightLogicalPlan)
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
      // val answerRDD = join(leftTrieRDD, rightTrieRDD, distanceFunction, threshold)
      val answerRDD = biJoin(leftTrieRDD, rightTrieRDD, distanceFunction, threshold)
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

  private def join(leftTrieRDD: TrieRDD, rightTrieRDD: TrieRDD,
                   distanceFunction: TrajectorySimilarity, threshold: Double):
  RDD[(Trajectory, Trajectory)] = {
    val leftGlobalTrieIndex = leftTrieRDD.globalIndex.asInstanceOf[GlobalTrieIndex]
    val bLeftGlobalTrieIndex = sparkContext.broadcast(leftGlobalTrieIndex)
    val leftNumPartitions = leftTrieRDD.packedRDD.partitions.length

    // get right candidates RDD
    val rightCandidatesRDD = rightTrieRDD.packedRDD.flatMap(packedPartition =>
      packedPartition.data.asInstanceOf[Array[Trajectory]].flatMap(t => {
        val candidatePartitionIdxs = bLeftGlobalTrieIndex.value.getPartitions(t, threshold)
        candidatePartitionIdxs.map(candidatePartitionIdx => (candidatePartitionIdx, t))
      })
    )
    val partitionedRightCandidatesRDD = ExactKeyPartitioner.partitionWithToZipRDD(
      rightCandidatesRDD, leftNumPartitions, leftTrieRDD.packedRDD)
    val sampledPartitionedRightCandidatesRDD = partitionedRightCandidatesRDD
      .sample(withReplacement = true, DITAConfigConstants.BALANCING_SAMPLE_RATE)

    // get left partition cost
    val leftPartitionCost = leftTrieRDD.packedRDD.zipPartitions(sampledPartitionedRightCandidatesRDD)
    { case (partitionIter, trajectoryIter) =>
      val packedPartition = partitionIter.next()
      val localTrieIndex = packedPartition.indexes
        .filter(_.isInstanceOf[LocalTrieIndex]).head.asInstanceOf[LocalTrieIndex]
      val candidatesCount = trajectoryIter.map(t => localTrieIndex.getCandidates(t, threshold).size)
        .sum
      Array((packedPartition.id, candidatesCount)).iterator
    }.collect()

    // get balancing value
    val sortedLeftPartitionCost = leftPartitionCost.sortBy(_._2)
    var standardValueForBalancing: Int = sortedLeftPartitionCost(
      (leftNumPartitions * DITAConfigConstants.BALANCING_PERCENTILE).toInt)._2
    if (standardValueForBalancing == 0) {
      standardValueForBalancing = Int.MaxValue
    }
    val balancingPartitions = leftPartitionCost.filter(_._2 >= standardValueForBalancing)
      .map(x => {
        val partitionId = x._1
        val totalCostForPartition = x._2
        val balancingCount = totalCostForPartition / standardValueForBalancing * 2 + 1
        (partitionId, balancingCount)
      }).toMap
    val bBalancingPartitions = sparkContext.broadcast(balancingPartitions)

    val normalPartitionedRightCandidatesRDD = partitionedRightCandidatesRDD
      .mapPartitionsWithIndex((idx, iter) =>
      if (!bBalancingPartitions.value.contains(idx)) iter else Iterator())
    val normalAnswerRDD = leftTrieRDD.packedRDD.zipPartitions(normalPartitionedRightCandidatesRDD)
    { case (partitionIter, trajectoryIter) =>
      localJoin(partitionIter, trajectoryIter, distanceFunction, threshold)
    }

    if (balancingPartitions.isEmpty) {
      normalAnswerRDD
    } else {
      var balancingPartitionCount = 0
      val balancingPartitionsWithIndex = balancingPartitions.map
      { case (partitionIdx, balancingCount) =>
        val balancingPartitionWithIndex = (partitionIdx, (balancingPartitionCount, balancingCount))
        balancingPartitionCount += balancingCount
        balancingPartitionWithIndex
      }
      val bBalancingPartitionsWithIndex = sparkContext.broadcast(balancingPartitionsWithIndex)

      val balancingLeftDataRDD = balancingPartitionsWithIndex.map
      { case (partitionIdx, (balancingPartitionIdx, balancingCount)) =>
        new PartitionPruningRDD(leftTrieRDD.packedRDD, _ == partitionIdx)
          .flatMap(packedPartition => {
            (0 until balancingCount).map(i => (balancingPartitionIdx + i, packedPartition))
          })
      }
      val balancingLeftRDD = ExactKeyPartitioner.partition(
        balancingLeftDataRDD.reduce((x, y) => x.union(y)), balancingPartitionCount)

      val balancingRightCandidatesRDD = partitionedRightCandidatesRDD.mapPartitionsWithIndex(
        (idx, iter) => {
          if (bBalancingPartitionsWithIndex.value.contains(idx)) {
            val (balancingPartitionIdx, balancingCount) = bBalancingPartitionsWithIndex.value(idx)
            iter.map(t => (balancingPartitionIdx +
              (t.hashCode() % balancingCount + balancingCount) % balancingCount, t))
          } else {
            Iterator()
          }
        })
      val balancingPartitionedRightCandidatesRDD = ExactKeyPartitioner.partitionWithToZipRDD(
        balancingRightCandidatesRDD, balancingPartitionCount, balancingLeftRDD)

      val balancingAnswerRDD = balancingLeftRDD
        .zipPartitions(balancingPartitionedRightCandidatesRDD)
        { case (partitionIter, trajectoryIter) =>
          localJoin(partitionIter, trajectoryIter, distanceFunction, threshold)
        }

      normalAnswerRDD.union(balancingAnswerRDD)
    }
  }

  private def biJoin(leftTrieRDD: TrieRDD, rightTrieRDD: TrieRDD,
                   distanceFunction: TrajectorySimilarity, threshold: Double):
  RDD[(Trajectory, Trajectory)] = {
    // basic variables
    val leftNumPartitions = leftTrieRDD.packedRDD.partitions.length
    val rightNumPartitions = rightTrieRDD.packedRDD.partitions.length
    var start = System.currentTimeMillis()
    var end = System.currentTimeMillis()

    // get cost
    start = System.currentTimeMillis()
    val (transCost, compCost) = getCost(leftTrieRDD, rightTrieRDD, threshold)
    end = System.currentTimeMillis()
    logWarning(s"Computing cost time: ${(end - start) / 1000}s")

    // construct the graph
    start = System.currentTimeMillis()
    val totalNumPartitions = leftNumPartitions + rightNumPartitions
    val allEdges = (0 until totalNumPartitions).map(partitionId => {
      val edges = if (partitionId < leftNumPartitions) {
        val leftPartitionId = partitionId
        transCost.filterKeys(key => key.startsWith(s"L$leftPartitionId")).map(x => {
          val rightPartitionId = x._1.drop(x._1.indexOf("R") + 1).toInt
          val transWeight = x._2
          val compWeight = compCost.getOrElse(x._1, 0)
          (rightPartitionId + leftNumPartitions, (transWeight, compWeight))
        })
      } else {
        val rightPartitionId = partitionId - leftNumPartitions
        transCost.filterKeys(key => key.startsWith(s"R$rightPartitionId")).map(x => {
          val leftPartitionId = x._1.drop(x._1.indexOf("L") + 1).toInt
          val transWeight = x._2
          val compWeight = compCost.getOrElse(x._1, 0)
          (leftPartitionId, (transWeight, compWeight))
        })
      }
      (partitionId, edges)
    }).toMap

    // employ graph orientation and load balancing
    val (edgeDirection, balancingPartitions) = balanceGraph(allEdges, totalNumPartitions)
    val left2RightEdges = edgeDirection.flatMap(x => {
      val partitionId1 = x._1
      val partitionId2 = x._2
      if (partitionId1 < leftNumPartitions) {
        Some((partitionId1, partitionId2 - leftNumPartitions))
      } else {
        None
      }
    })
    val right2LeftEdges = edgeDirection.flatMap(x => {
      val partitionId1 = x._1
      val partitionId2 = x._2
      if (partitionId1 >= leftNumPartitions) {
        Some((partitionId1 - leftNumPartitions, partitionId2))
      } else {
        None
      }
    })
    // assert(edgeDirection.forall(t => !edgeDirection.contains((t._2, t._1))))
    // assert(left2RightEdges.forall(t => !right2LeftEdges.contains((t._2, t._1))))
    // assert(right2LeftEdges.forall(t => !left2RightEdges.contains((t._2, t._1))))
    end = System.currentTimeMillis()
    logWarning(s"Computing optimal graph time: ${(end - start) / 1000}s")

    // get balancing partitions
    val leftBalancingPartitions = balancingPartitions
      .filterKeys(partitionId => partitionId < leftNumPartitions)
    val rightBalancingPartitions = balancingPartitions
      .filterKeys(partitionId => partitionId >= leftNumPartitions)
      .map(x => (x._1 - leftNumPartitions, x._2))

    // run join
    val right2LeftAnswerRDD = getJoinedRDD(leftTrieRDD, rightTrieRDD,
      leftBalancingPartitions, left2RightEdges, right2LeftEdges, distanceFunction, threshold)
    val left2RightAnswerRDD = getJoinedRDD(rightTrieRDD, leftTrieRDD,
      rightBalancingPartitions, right2LeftEdges, left2RightEdges, distanceFunction, threshold, true)

    right2LeftAnswerRDD.union(left2RightAnswerRDD)
  }

  private def getCost(leftTrieRDD: TrieRDD, rightTrieRDD: TrieRDD, threshold: Double):
  (Map[String, Int], Map[String, Int]) = {
    // basic variables
    val leftGlobalTrieIndex = leftTrieRDD.globalIndex.asInstanceOf[GlobalTrieIndex]
    val bLeftGlobalTrieIndex = sparkContext.broadcast(leftGlobalTrieIndex)
    val leftNumPartitions = leftTrieRDD.packedRDD.partitions.length

    val rightGlobalTrieIndex = rightTrieRDD.globalIndex.asInstanceOf[GlobalTrieIndex]
    val bRightGlobalTrieIndex = sparkContext.broadcast(rightGlobalTrieIndex)
    val rightNumPartitions = rightTrieRDD.packedRDD.partitions.length

    // TODO: Use New Interface
    // define cost model
    implicit val hashMapParam = new HashMapParam
    val transCost: Accumulable[HashMap[String, Int], (String, Int)] =
      sparkContext.accumulable[HashMap[String, Int], (String, Int)](
        HashMap.empty[String, Int])(hashMapParam)
    val compCost: Accumulable[HashMap[String, Int], (String, Int)] =
      sparkContext.accumulable[HashMap[String, Int], (String, Int)](
        HashMap.empty[String, Int])(hashMapParam)

    // get sampled candidates and compute transmission cost
    val balancingSampleRate = DITAConfigConstants.BALANCING_SAMPLE_RATE
    val sampledLeftCandidatesRDD = leftTrieRDD.packedRDD.flatMap(packedPartition =>
      packedPartition.getSample(balancingSampleRate).asInstanceOf[List[Trajectory]].flatMap(t => {
        val candidatePartitionIdxs = bRightGlobalTrieIndex.value.getPartitions(t, threshold)
        candidatePartitionIdxs.foreach(candidatePartitionIdx =>
          transCost.add((s"L${packedPartition.id}-R$candidatePartitionIdx", 1)))
        candidatePartitionIdxs.map(candidatePartitionIdx => (candidatePartitionIdx, t))
      })
    )
    sampledLeftCandidatesRDD.count()

    val sampledRightCandidatesRDD = rightTrieRDD.packedRDD.flatMap(packedPartition =>
      packedPartition.getSample(balancingSampleRate).asInstanceOf[List[Trajectory]].flatMap(t => {
        val candidatePartitionIdxs = bLeftGlobalTrieIndex.value.getPartitions(t, threshold)
        candidatePartitionIdxs.foreach(candidatePartitionIdx =>
          transCost.add((s"R${packedPartition.id}-L$candidatePartitionIdx", 1)))
        candidatePartitionIdxs.map(candidatePartitionIdx => (candidatePartitionIdx, t))
      })
    )
    sampledLeftCandidatesRDD.count()

    // get sampled joined data and compute computation cost
    val partitionedSampledRightCandidatesRDD = ExactKeyPartitioner.partitionWithToZipRDD(
      sampledRightCandidatesRDD, leftNumPartitions, leftTrieRDD.packedRDD)
    leftTrieRDD.packedRDD.zipPartitions(partitionedSampledRightCandidatesRDD)
    { case (partitionIter, trajectoryIter) =>
        val packedPartition = partitionIter.next()
        val localTrieIndex = packedPartition.indexes
          .filter(_.isInstanceOf[LocalTrieIndex]).head.asInstanceOf[LocalTrieIndex]
        trajectoryIter.foreach(t => {
          val rightPartitionIdx = bRightGlobalTrieIndex.value.getPartitions(t, 0.0).head
          compCost.add((s"R$rightPartitionIdx-L${packedPartition.id}",
            localTrieIndex.getCandidates(t, threshold).size))
        })
        Array(packedPartition.id).iterator
    }.count()

    val partitionedSampledLeftCandidatesRDD = ExactKeyPartitioner.partitionWithToZipRDD(
      sampledLeftCandidatesRDD, rightNumPartitions, rightTrieRDD.packedRDD)
    rightTrieRDD.packedRDD.zipPartitions(partitionedSampledLeftCandidatesRDD)
    { case (partitionIter, trajectoryIter) =>
        val packedPartition = partitionIter.next()
        val localTrieIndex = packedPartition.indexes
          .filter(_.isInstanceOf[LocalTrieIndex]).head.asInstanceOf[LocalTrieIndex]
        trajectoryIter.foreach(t => {
          val leftPartitionIdx = bLeftGlobalTrieIndex.value.getPartitions(t, 0.0).head
          compCost.add((s"L$leftPartitionIdx-R${packedPartition.id}",
            localTrieIndex.getCandidates(t, threshold).size))
        })
        Array(packedPartition.id).iterator
    }.count()

    (transCost.value.toMap, compCost.value.toMap)
  }

  private def balanceGraph(allEdges: Map[Int, Map[Int, (Int, Int)]],
                           totalNumPartitions: Int): (Set[(Int, Int)], Map[Int, Int]) = {

    // initialize edge direction
    var edgeDirection = (0 until totalNumPartitions).flatMap(partitionId1 => {
      val edges = allEdges.getOrElse(partitionId1, Map.empty)
      edges.map(y => {
        val partitionId2 = y._1
        val (transWeight12, compWeight12) = y._2
        val (transWeight21, compWeight21) = getWeight(allEdges, partitionId2, partitionId1)

        // compare the costs of two directions to get the initial direction
        if (getTotalCost(transWeight12, compWeight12) < getTotalCost(transWeight21, compWeight21)) {
          (partitionId1, partitionId2)
        } else if (getTotalCost(transWeight12, compWeight12)
          == getTotalCost(transWeight21, compWeight21)) {
          (math.min(partitionId1, partitionId2), math.max(partitionId1, partitionId2))
        } else {
          (partitionId2, partitionId1)
        }
      })
    }).toSet

    // get total cost for each partition
    val totalCost = (0 until totalNumPartitions).map(partitionId1 =>
      getTotalCostForPartition(partitionId1, allEdges, edgeDirection)
    ).toArray
    logWarning(s"Initial maximum total cost: ${totalCost.max}")

    // greedy algorithm for chaning edge direction
    val loop = new Breaks
    import loop.{break, breakable}
    breakable {
      while (true) {
        val partitionId = totalCost.indices.maxBy(totalCost)

        // find edge with biggest gain
        val edges = edgeDirection.filter(x => x._1 == partitionId || x._2 == partitionId)
        val edgeCosts = edges.map(x => {
          val (transWeight12, compWeight12) = getWeight(allEdges, x._1, x._2)
          val (transWeight21, compWeight21) = getWeight(allEdges, x._2, x._1)
          if (x._1 == partitionId) {
            (x, getTotalCost(0, compWeight21) - getTotalCost(transWeight12, 0))
          } else {
            (x, getTotalCost(transWeight21, 0) - getTotalCost(0, compWeight12))
          }
        })
        val (edge, gain) = edgeCosts.maxBy(_._2)
        if (gain <= 0) {
          break
        }

        // change edge direction
        edgeDirection -= edge
        edgeDirection += Tuple2(edge._2, edge._1)

        // update totalCost
        val lastMaxCost = totalCost.max
        totalCost(edge._1) = getTotalCostForPartition(edge._1, allEdges, edgeDirection)
        totalCost(edge._2) = getTotalCostForPartition(edge._2, allEdges, edgeDirection)
        if (totalCost.max >= lastMaxCost) {
          edgeDirection -= Tuple2(edge._2, edge._1)
          edgeDirection += edge
          totalCost(edge._1) = getTotalCostForPartition(edge._1, allEdges, edgeDirection)
          totalCost(edge._2) = getTotalCostForPartition(edge._2, allEdges, edgeDirection)
          break
        }
      }
    }
    logWarning(s"Maximum total cost: ${totalCost.max}")

    // balancing
    val sortedTotalCost = totalCost.sorted
    var standardValueForBalancing: Int =
      sortedTotalCost((totalNumPartitions * DITAConfigConstants.BALANCING_PERCENTILE).toInt)
    if (standardValueForBalancing == 0) {
      standardValueForBalancing = Int.MaxValue
    }
    val balancingPartitions = totalCost.zipWithIndex.filter(_._1 >= standardValueForBalancing)
      .map(x => {
        val partitionId = x._2
        val totalCostForPartition = x._1
        val balancingCount = totalCostForPartition / standardValueForBalancing * 2 + 1
        (partitionId, balancingCount)
      }).toMap

    (edgeDirection, balancingPartitions)
  }

  private def getWeight(allEdges: Map[Int, Map[Int, (Int, Int)]],
                partitionId1: Int, partitionId2: Int): (Int, Int) = {
    allEdges.getOrElse(partitionId1, Map.empty).getOrElse(partitionId2, (0, 0))
  }

  private def getTotalCost(transWeight: Int, compWeight: Int): Int = {
    (transWeight * LAMBDA + compWeight).toInt
  }

  private def getTotalCostForPartition(partitionId1: Int, allEdges: Map[Int, Map[Int, (Int, Int)]],
                               edgeDirection: Set[(Int, Int)]): Int = {
    val edges = allEdges.getOrElse(partitionId1, Map.empty)
    edges.map(y => {
      val partitionId2 = y._1
      val (transWeight12, _) = y._2
      val (_, compWeight21) = getWeight(allEdges, partitionId2, partitionId1)

      if (edgeDirection.contains((partitionId1, partitionId2))) {
        getTotalCost(transWeight12, 0)
      } else {
        getTotalCost(0, compWeight21)
      }
    }).sum
  }

  private def getJoinedRDD(leftTrieRDD: TrieRDD, rightTrieRDD: TrieRDD,
                           balancingPartitions: Map[Int, Int],
                           left2RightEdges: Set[(Int, Int)],
                           right2LeftEdges: Set[(Int, Int)],
                           distanceFunction: TrajectorySimilarity,
                           threshold: Double,
                           shouldSendIfNotSpecified: Boolean = false):
  RDD[(Trajectory, Trajectory)] = {
    val leftNumPartitions = leftTrieRDD.packedRDD.partitions.length
    val globalTrieIndex = leftTrieRDD.globalIndex.asInstanceOf[GlobalTrieIndex]
    val bGlobalTrieIndex = sparkContext.broadcast(globalTrieIndex)
    val bLeft2RightEdges = sparkContext.broadcast(HashSet(left2RightEdges.toSeq: _*))
    val bRight2LeftEdges = sparkContext.broadcast(HashSet(right2LeftEdges.toSeq: _*))
    val bBalancingPartitions = sparkContext.broadcast(HashMap(balancingPartitions.toSeq: _*))

    // candidates
    val rightCandidatesRDD = rightTrieRDD.packedRDD.flatMap(packedPartition =>
      packedPartition.data.asInstanceOf[Array[Trajectory]].flatMap(t => {
        val candidatePartitionIdxs = bGlobalTrieIndex.value.getPartitions(t, threshold)
        val sentCandidatePartitionIdxs = candidatePartitionIdxs.filter(candidatePartitionIdx =>
          bRight2LeftEdges.value.contains((packedPartition.id, candidatePartitionIdx)) ||
            (!bLeft2RightEdges.value.contains(candidatePartitionIdx, packedPartition.id)
              && shouldSendIfNotSpecified))
        sentCandidatePartitionIdxs.map(candidatePartitionIdx => (candidatePartitionIdx, t))
      })
    )
    val partitionedRightCandidatesRDD = ExactKeyPartitioner.partitionWithToZipRDD(
      rightCandidatesRDD, leftNumPartitions, leftTrieRDD.packedRDD)

    // normal answer
    val normalPartitionedRightCandidatesRDD = partitionedRightCandidatesRDD.mapPartitionsWithIndex((idx, iter) =>
      if (!bBalancingPartitions.value.contains(idx)) iter else Iterator())
    val normalAnswerRDD = leftTrieRDD.packedRDD.zipPartitions(normalPartitionedRightCandidatesRDD)
    { case (partitionIter, trajectoryIter) =>
      localJoin(partitionIter, trajectoryIter, distanceFunction, threshold)
    }

    // balancing answer
    if (balancingPartitions.isEmpty) {
      normalAnswerRDD
    } else {
      var balancingPartitionCount = 0
      val balancingPartitionsWithIndex = balancingPartitions.map
      { case (partitionIdx, balancingCount) =>
        val balancingPartitionWithIndex = (partitionIdx, (balancingPartitionCount, balancingCount))
        balancingPartitionCount += balancingCount
        balancingPartitionWithIndex
      }
      val bBalancingPartitionsWithIndex = sparkContext.broadcast(balancingPartitionsWithIndex)

      val balancingLeftDataRDD = balancingPartitionsWithIndex.map
      { case (partitionIdx, (balancingPartitionIdx, balancingCount)) =>
        new PartitionPruningRDD(leftTrieRDD.packedRDD, _ == partitionIdx)
          .flatMap(packedPartition => {
            (0 until balancingCount).map(i => (balancingPartitionIdx + i, packedPartition))
          })
      }
      val balancingLeftRDD = ExactKeyPartitioner.partition(
        balancingLeftDataRDD.reduce((x, y) => x.union(y)), balancingPartitionCount)

      val balancingRightCandidatesRDD = partitionedRightCandidatesRDD.mapPartitionsWithIndex(
        (idx, iter) => {
          if (bBalancingPartitionsWithIndex.value.contains(idx)) {
            val (balancingPartitionIdx, balancingCount) = bBalancingPartitionsWithIndex.value(idx)
            iter.map(t => (balancingPartitionIdx +
              (t.hashCode() % balancingCount + balancingCount) % balancingCount, t))
          } else {
            Iterator()
          }
        })
      val balancingPartitionedRightCandidatesRDD = ExactKeyPartitioner.partitionWithToZipRDD(
        balancingRightCandidatesRDD, balancingPartitionCount, balancingLeftRDD)

      val balancingAnswerRDD = balancingLeftRDD
        .zipPartitions(balancingPartitionedRightCandidatesRDD)
      { case (partitionIter, trajectoryIter) =>
        localJoin(partitionIter, trajectoryIter, distanceFunction, threshold)
      }

      normalAnswerRDD.union(balancingAnswerRDD)
    }
  }

  private def localJoin(partitionIter: Iterator[PackedPartition],
                        trajectoryIter: Iterator[Trajectory],
                        distanceFunction: TrajectorySimilarity,
                        threshold: Double): Iterator[(Trajectory, Trajectory)] = {
    val packedPartition = partitionIter.next()
    val localTreeIndex = packedPartition.indexes.filter(_.isInstanceOf[LocalTrieIndex]).head
      .asInstanceOf[LocalTrieIndex]
    val queryTrajectories = trajectoryIter.toList
    queryTrajectories.foreach(_.refresh(threshold))

    val answerPairs = queryTrajectories.flatMap(query => {
      val indexCandidates = localTreeIndex.getCandidates(query, threshold)
      val mbrCandidates = indexCandidates.filter(t => query.getExtendedMBR.contains(t.getMBR))
      mbrCandidates.filter(t =>
        distanceFunction.evalWithTrajectory(query, t, threshold) <= threshold)
        .map(t => (query, t))
    })

    answerPairs.iterator
  }

  private def getIndexedRelation(plan: LogicalPlan): Option[IndexedRelation] = {
    sqlContext.sessionState.indexRegistry.lookupIndex(plan).map(_.relation)
  }
}
