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
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle}

// scalastyle:off println
object DITASQLExample {

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

    val trajs = spark.sparkContext
      .textFile("examples/src/main/resources/trajectory_small.txt")
      .zipWithIndex().map(getTrajectory)
      .filter(_.traj.length >= DITAConfigConstants.TRAJECTORY_MIN_LENGTH)
      .filter(_.traj.length <= DITAConfigConstants.TRAJECTORY_MAX_LENGTH)
    val df = trajs.toDF()
    df.createOrReplaceTempView("traj1")
    df.createOrReplaceTempView("traj2")

    // create index for traj1
    var start = System.currentTimeMillis()
    spark.sql("CREATE TRIE INDEX traj1_index ON traj1 (traj)")
    var end = System.currentTimeMillis()
    println(s"Building Index time: ${end - start} ms")

    // create index for traj2
    start = System.currentTimeMillis()
    spark.sql("CREATE TRIE INDEX traj2_index ON traj2 (traj)")
    end = System.currentTimeMillis()
    println(s"Building Index time: ${end - start} ms")

    // create index
    start = System.currentTimeMillis()
    spark.sql("CREATE TRIE INDEX traj1_index ON traj1 (traj)")
    end = System.currentTimeMillis()
    println(s"Building Index time: ${end - start} ms")

    // trajectory similarity search with threshold
    start = System.currentTimeMillis()
    var queryTrajStr = trajs.take(1).head.traj
      .map(point => s"POINT(${point.mkString(",")})").mkString(",")
    queryTrajStr = s"TRAJECTORY($queryTrajStr)"
    spark.sql(s"SELECT COUNT(*) FROM traj1 WHERE DTW(traj1.traj, $queryTrajStr) <= 0.005").show()
    end = System.currentTimeMillis()
    println(s"Threshold Search Running time: ${end - start} ms")

    // trajectory similarity search with knn
    start = System.currentTimeMillis()
    spark.sql(s"SELECT COUNT(*) FROM traj1 WHERE DTW(traj1.traj, $queryTrajStr) KNN 100").show()
    end = System.currentTimeMillis()
    println(s"KNN Search Running time: ${end - start} ms")

    // trajectory similarity join with threshold
    start = System.currentTimeMillis()
    spark.sql("SELECT COUNT(*) FROM traj1 JOIN traj2 ON DTW(traj1.traj, traj2.traj) <= 0.005")
      .show()
    end = System.currentTimeMillis()
    println(s"Threshold Join Running time: ${end - start} ms")

    // trajectory similarity join with knn
    start = System.currentTimeMillis()
    spark.sql("SELECT COUNT(*) FROM traj1 JOIN traj2 ON DTW(traj1.traj, traj2.traj) KNN 100")
      .show()
    end = System.currentTimeMillis()
    println(s"KNN Join Running time: ${end - start} ms")

    // trajectory mbr range search
    start = System.currentTimeMillis()
    val mbr = Rectangle(Point(Array(39.8, 116.2)), Point(Array(40.0, 116.4)))
    spark.sql(s"SELECT COUNT(*) FROM traj1 WHERE traj1.traj IN MBRRANGE" +
      s"(POINT(${mbr.low.coord.mkString(",")}), POINT(${mbr.high.coord.mkString(",")}))")
      .show()
    end = System.currentTimeMillis()
    println(s"MBR Range Search Running time: ${end - start} ms")

    // trajectory circle range search
    start = System.currentTimeMillis()
    val center = Point(Array(39.9, 116.3))
    val radius = 0.1
    spark.sql(s"SELECT COUNT(*) FROM traj1 WHERE traj1.traj IN CIRCLERANGE" +
      s"(POINT(${center.coord.mkString(",")}), $radius)")
      .show()
    end = System.currentTimeMillis()
    println(s"Circle Range Search Running time: ${end - start} ms")

    spark.stop()
  }
}
