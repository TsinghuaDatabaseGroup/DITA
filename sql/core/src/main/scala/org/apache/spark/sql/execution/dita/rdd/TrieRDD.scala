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

package org.apache.spark.sql.execution.dita.rdd

import scala.collection.mutable.ArrayBuffer

import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory
import org.apache.spark.sql.execution.dita.index.{GlobalIndex, LocalIndex}
import org.apache.spark.sql.execution.dita.index.global.GlobalTrieIndex
import org.apache.spark.sql.execution.dita.index.local.LocalTrieIndex
import org.apache.spark.sql.execution.dita.partition.PackedPartition
import org.apache.spark.sql.execution.dita.partition.global.GlobalTriePartitioner
import org.apache.spark.storage.StorageLevel

class TrieRDD(dataRDD: RDD[Trajectory]) {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  var packedRDD: RDD[PackedPartition] = _
  var globalIndex: GlobalIndex = _

  private def buildIndex(): Unit = {
    var start = System.currentTimeMillis()
    var end = start

    // get tree partition
    start = System.currentTimeMillis()
    val (partitionedRDD, partitioner) = GlobalTriePartitioner.partition(dataRDD)
    end = System.currentTimeMillis()
    LOG.warn(s"Trie Partitioning Time: ${end - start} ms")

    // build local index
    start = System.currentTimeMillis()
    packedRDD = partitionedRDD.mapPartitionsWithIndex { case (index, iter) =>
      val data = iter.toArray
      val indexes = ArrayBuffer.empty[LocalIndex]
      indexes.append(LocalTrieIndex.buildIndex(data))
      Array(PackedPartition(index, data, indexes.toArray)).iterator
    }
    packedRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    packedRDD.count()
    end = System.currentTimeMillis()
    LOG.warn(s"Building Local Index Time: ${end - start} ms")

    // log statistics
    val partitionSizes = packedRDD.mapPartitions(iter => iter.map(_.data.length)).collect()
    LOG.warn(s"Tree Partitions Count: ${partitionSizes.length}")
    LOG.warn(s"Tree Partitions Sizes: ${partitionSizes.mkString(",")}")
    LOG.warn(s"Max Partition Size: ${partitionSizes.max}")
    LOG.warn(s"Min Partition Size: ${partitionSizes.min}")
    LOG.warn(s"Avg Partition Size: ${partitionSizes.sum / partitionSizes.length}")
    val sortedPartitionSizes = partitionSizes.sorted
    LOG.warn(s"5% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.05).toInt)}")
    LOG.warn(s"25% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.25).toInt)}")
    LOG.warn(s"50% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.5).toInt)}")
    LOG.warn(s"75% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.75).toInt)}")
    LOG.warn(s"95% Partition Size: ${sortedPartitionSizes((partitionSizes.length * 0.95).toInt)}")

    // build global index
    start = System.currentTimeMillis()
    val globalTreeIndex = GlobalTrieIndex(partitioner)
    globalIndex = globalTreeIndex
    end = System.currentTimeMillis()
    LOG.warn(s"Building Global Index Time: ${end - start} ms")
  }

  buildIndex()
}