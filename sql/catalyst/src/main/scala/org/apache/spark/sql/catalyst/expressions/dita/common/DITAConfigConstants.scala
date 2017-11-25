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

package org.apache.spark.sql.catalyst.expressions.dita.common

import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.{Trajectory, TrajectorySimilarity}

object DITAConfigConstants {
  // basic
  val THRESHOLD_LIMIT = 100.0
  val DISTANCE_FUNCTION: TrajectorySimilarity = TrajectorySimilarity.DTWDistance
  val TRAJECTORY_MIN_LENGTH = 6
  val TRAJECTORY_MAX_LENGTH = 1000

  // rtree
  val SAMPLE_RATE = 0.05
  val MIN_SAMPLE_SIZE: Long = 1L * 1024 * 1024
  val MAX_SAMPLE_SIZE: Long = 2048L * 1024 * 1024
  val GLOBAL_MAX_ENTRIES_PER_NODE = 20
  val LOCAL_MAX_ENTRIES_PER_NODE = 20

  // global
  var GLOBAL_NUM_PARTITIONS = 8
  var GLOBAL_INDEXED_PIVOT_COUNT: Int = 0
  var GLOBAL_PIVOT_NUM_PARTITIONS = 2

  // local
  var LOCAL_NUM_PARTITIONS = 32
  var LOCAL_INDEXED_PIVOT_COUNT = 4
  var LOCAL_PIVOT_NUM_PARTITIONS = 1
  val LOCAL_PIVOT_MAX_DATA_COUNT = 0
  var LOCAL_CELL_SIZE = 0.001
  var LOCAL_CELL_THRESHOLD = 0.1
  var LOCAL_MIN_NODE_SIZE = 16

  // load balancing
  var BALANCING_PERCENTILE = 0.95
  var BALANCING_SAMPLE_RATE = 0.01
  val BALANCING_MIN_SAMPLE_SIZE = 1000

  // knn
  var KNN_COEFFICIENT = 3
  var KNN_MAX_LOCAL_ITERATION = 10
  var KNN_EPSILON: Double = 1e-6
  var KNN_MAX_GLOBAL_ITERATION = 3
  val KNN_MAX_SAMPLING_RATE = 0.1

  /*
  def loadFromConfig(confFile: String): Unit = {
    val conf = ConfigFactory.load(confFile)

    GLOBAL_NUM_PARTITIONS = conf.getInt("global_num_partitions")
    GLOBAL_INDEXED_PIVOT_COUNT = conf.getInt("global_indexed_pivot_count")
    GLOBAL_PIVOT_NUM_PARTITIONS = conf.getInt("global_pivot_num_partitions")

    LOCAL_NUM_PARTITIONS = conf.getInt("local_num_partitions")
    LOCAL_INDEXED_PIVOT_COUNT = conf.getInt("local_indexed_pivot_count")
    LOCAL_PIVOT_NUM_PARTITIONS = conf.getInt("local_pivot_num_partitions")
    LOCAL_CELL_SIZE = conf.getDouble("local_cell_size")
    LOCAL_CELL_THRESHOLD = conf.getDouble("local_cell_threshold")
    LOCAL_MIN_NODE_SIZE = conf.getInt("locla_min_node_size")

    BALANCING_PERCENTILE = conf.getDouble("balancing_percentile")
    BALANCING_SAMPLE_RATE = conf.getDouble("balancing_sample_rate")
    BALANCING_SAMPLE_SIZE = conf.getInt("balancing_sample_size")
  }
  */
}
