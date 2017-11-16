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
import org.apache.spark.sql.catalyst.expressions.dita.TrajectorySimilarityFunction

object DITADataFrameExample {

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

    val df1 = spark.sparkContext
      .textFile("examples/src/main/resources/trajectory_small.txt")
      .zipWithIndex().map(getTrajectory)
      .toDF()
    df1.createOrReplaceTempView("traj1")
    df1.createTrieIndex(df1("traj"), "traj_index1")

    val df2 = spark.sparkContext
      .textFile("examples/src/main/resources/trajectory_small.txt")
      .zipWithIndex().map(getTrajectory)
      .toDF()
    df2.createTrieIndex(df2("traj"), "traj_index2")

    df1.trajSimJoin(df2, df1("traj"), df2("traj"), TrajectorySimilarityFunction.DTW, 0.005).show()

    spark.stop()
  }
}
