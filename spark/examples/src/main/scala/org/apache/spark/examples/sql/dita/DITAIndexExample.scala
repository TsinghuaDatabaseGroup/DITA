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

package org.apache.spark.examples.sql.dita

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.dita.common.DITAConfigConstants

object DITAIndexExample {

  case class TrajectoryRecord(id: Long, traj: Array[Array[Double]])

  private def getTrajectory(line: (String, Long)): TrajectoryRecord = {
    val points = line._1.split(";").map(_.split(","))
      .map(x => x.map(_.toDouble))
    TrajectoryRecord(line._2, points)
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.sparkContext
      .textFile("examples/src/main/resources/trajectory.txt")
      .zipWithIndex().map(getTrajectory)
      .filter(_.traj.length >= DITAConfigConstants.TRAJECTORY_MIN_LENGTH)
      .filter(_.traj.length <= DITAConfigConstants.TRAJECTORY_MAX_LENGTH)
      .toDF()
    df.createOrReplaceTempView("traj1")

    spark.sql("SHOW TRIE INDEXES").show()
    spark.sql("CREATE TRIE INDEX traj1_index ON traj1 (traj)")
    spark.sql("SHOW TRIE INDEXES").show()

    spark.sql("DROP TRIE INDEX traj1_index ON traj1")
    spark.sql("SHOW TRIE INDEXES").show()

    df.createTrieIndex(df("traj"), "traj1_index")
    spark.sql("SHOW TRIE INDEXES").show()

    df.dropTrieIndex(df("traj"))
    spark.sql("SHOW TRIE INDEXES").show()
  }
}
