package tsinghua.dita

import org.apache.spark.sql.SparkSession
import tsinghua.dita.algorithms.{TrajectoryRangeAlgorithms, TrajectorySimilarityWithKNNAlgorithms, TrajectorySimilarityWithThresholdAlgorithms}
import tsinghua.dita.common.DITAConfigConstants
import tsinghua.dita.common.shape.{Point, Rectangle}
import tsinghua.dita.common.trajectory.{Trajectory, TrajectorySimilarity}
import tsinghua.dita.rdd.TrieRDD

object ExampleApp {

  private def getTrajectory(line: (String, Long)): Trajectory = {
    val points = line._1.split(";").map(_.split(","))
      .map(x => Point(x.map(_.toDouble)))
    Trajectory(points)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val trajs = spark.sparkContext
      .textFile("src/main/resources/trajectory.txt")
      .zipWithIndex().map(getTrajectory)
      .filter(_.points.length >= DITAConfigConstants.TRAJECTORY_MIN_LENGTH)
      .filter(_.points.length <= DITAConfigConstants.TRAJECTORY_MAX_LENGTH)
    println(s"Trajectory count: ${trajs.count()}")

    val rdd1 = new TrieRDD(trajs)
    val rdd2 = new TrieRDD(trajs)

    // threshold-based search
    val queryTrajectory = trajs.take(1).head
    val thresholdSearch = TrajectorySimilarityWithThresholdAlgorithms.DistributedSearch
    val thresholdSearchAnswer = thresholdSearch.search(spark.sparkContext, queryTrajectory, rdd1, TrajectorySimilarity.DTWDistance, 0.005)
    println(s"Threshold search answer count: ${thresholdSearchAnswer.count()}")

    // knn search
    val knnSearch = TrajectorySimilarityWithKNNAlgorithms.DistributedSearch
    val knnSearchAnswer = knnSearch.search(spark.sparkContext, queryTrajectory, rdd1, TrajectorySimilarity.DTWDistance, 100)
    println(s"KNN search answer count: ${knnSearchAnswer.count()}")

    // threshold-based join
    val thresholdJoin = TrajectorySimilarityWithThresholdAlgorithms.FineGrainedDistributedJoin
    val thresholdJoinAnswer = thresholdJoin.join(spark.sparkContext, rdd1, rdd2, TrajectorySimilarity.DTWDistance, 0.005)
    println(s"Threshold join answer count: ${thresholdJoinAnswer.count()}")

    // knn join
    val knnJoin = TrajectorySimilarityWithKNNAlgorithms.DistributedJoin
    val knnJoinAnswer = knnJoin.join(spark.sparkContext, rdd1, rdd2, TrajectorySimilarity.DTWDistance, 100)
    println(s"KNN join answer count: ${knnJoinAnswer.count()}")

    // mbr range search
    val search = TrajectoryRangeAlgorithms.DistributedSearch
    val mbr = Rectangle(Point(Array(39.8, 116.2)), Point(Array(40.0, 116.4)))
    val mbrAnswer = search.search(spark.sparkContext, mbr, rdd1, 0.0)
    println(s"MBR range search count: ${mbrAnswer.count()}")

    // circle range search
    val center = Point(Array(39.9, 116.3))
    val radius = 0.1
    val circleAnswer = search.search(spark.sparkContext, center, rdd1, radius)
    println(s"Circle range search count: ${circleAnswer.count()}")
  }
}
