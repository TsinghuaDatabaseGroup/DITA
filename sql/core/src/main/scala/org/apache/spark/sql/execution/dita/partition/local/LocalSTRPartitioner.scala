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

package org.apache.spark.sql.execution.dita.partition.local

import org.apache.spark.sql.catalyst.expressions.dita.common.DITAConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.execution.dita.partition.{Bounds, STRPartitioner}
import org.apache.spark.util.SizeEstimator

case class LocalSTRPartitioner(expectedNumPartitions: Int,
                               dimension: Int,
                               maxEntriesPerNode: Int,
                               data: Array[_ <: Product2[Point, Any]])
  extends STRPartitioner(expectedNumPartitions, dimension, maxEntriesPerNode) {


  override def getBoundsAndCount: (Bounds, Long) = {
    data.aggregate[(Bounds, Long)]((null, 0))((bound, data) => {
      val new_bound = if (bound._1 == null) {
        Bounds(data._1.coord, data._1.coord)
      } else {
        Bounds(bound._1.min.zip(data._1.coord).map(x => Math.min(x._1, x._2)),
          bound._1.max.zip(data._1.coord).map(x => Math.max(x._1, x._2)))
      }
      (new_bound, bound._2 + SizeEstimator.estimate(data._1))
    }, (left, right) => {
      val new_bound = {
        if (left._1 == null) right._1
        else if (right._1 == null) left._1
        else {
          Bounds(left._1.min.zip(right._1.min).map(x => Math.min(x._1, x._2)),
            left._1.max.zip(right._1.max).map(x => Math.max(x._1, x._2)))
        }}
      (new_bound, left._2 + right._2)
    })
  }

  override def getData(totalCount: Long): Array[Point] = data.map(_._1)
}

object LocalSTRPartitioner {
  private val maxEntriesPerNode = DITAConfigConstants.LOCAL_MAX_ENTRIES_PER_NODE

  def partition[T](data: Array[(Point, T)], dimension: Int, expectedNumPartitions: Int):
  (Array[Array[(Point, T)]], LocalSTRPartitioner) = {
    val partitioner = new LocalSTRPartitioner(expectedNumPartitions,
      dimension, maxEntriesPerNode, data)
    val shuffled = data.groupBy(p => partitioner.getPartition(p._1))
    ((0 until partitioner.numPartitions).map(i =>
      shuffled.getOrElse(i, Array.empty)).toArray, partitioner)
  }
}
