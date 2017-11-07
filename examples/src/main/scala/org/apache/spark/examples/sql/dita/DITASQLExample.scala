/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql.dita

import org.apache.spark.sql.SparkSession

object DITASQLExample {

  case class TrajectoryRecord(id: Long, traj: Array[Array[Double]])

  private def getTrajectory(line: (String, Long)): TrajectoryRecord = {
    val points = line._1.split(";").map(_.split(","))
      .map(x => x.map(_.toDouble))
    TrajectoryRecord(line._2, points)
  }

  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.sparkContext
      .textFile("examples/src/main/resources/trajectory.txt")
      .zipWithIndex().map(getTrajectory)
      .toDF()
    df.createOrReplaceTempView("traj1")
    df.createOrReplaceTempView("traj2")

    spark.sql("SELECT * FROM traj1 JOIN traj2 ON DTW(traj1.traj, traj2.traj) <= 0.005").show()

    spark.stop()
  }
}
