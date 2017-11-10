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

package org.apache.spark.sql.execution.dita.partition.global

import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

class ExactKeyPartitioner(expectedNumPartitions: Int) extends Partitioner {
  def numPartitions: Int = expectedNumPartitions
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    k
  }
}

object ExactKeyPartitioner {
  def partition[T: ClassTag](dataRDD: RDD[(Int, T)],
                            numPartitions: Int): RDD[T] = {
    val partitioner = new ExactKeyPartitioner(numPartitions)
    val shuffled = new ShuffledRDD[Int, T, T](dataRDD, partitioner)
    shuffled.map(_._2)
  }
}

