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

package org.apache.spark.sql.catalyst.expressions.dita.common.trajectory

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.dita.common.ConfigConstants
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle}

case class Trajectory(points: Array[Point]) {
  require(points.nonEmpty)

  override def toString: String = {
    s"Trajectory(points = ${points.mkString(",")}"
  }

  @transient
  private lazy val mbr: Rectangle = calcMBR()
  @transient
  private var extendedMBR: Rectangle = _
  @transient
  private lazy val globalIndexedPivot: Array[Point] =
    calcPivot(ConfigConstants.GLOBAL_INDEXED_PIVOT_COUNT).map(_._1)
  @transient
  private lazy val localIndexedPivot: Array[Point] =
    calcPivot(ConfigConstants.LOCAL_INDEXED_PIVOT_COUNT).map(_._1)
  @transient
  private lazy val cells: Array[(Rectangle, Int)] = calcCells()
  @transient
  private lazy val numDimensions = points.head.coord.length

  private def calcMBR(): Rectangle = {
    val dimension = numDimensions
    val min = (0 until dimension).map(i => points.map(_.coord(i)).min).toArray
    val max = (0 until dimension).map(i => points.map(_.coord(i)).max).toArray
    Rectangle(Point(min), Point(max))
  }

  private def calcExtendedMBR(threshold: Double): Rectangle = {
    Rectangle(Point(mbr.low.coord.map(_ - threshold)), Point(mbr.high.coord.map(_ + threshold)))
  }

  private def calcPivot(count: Int): Array[(Point, Int)] = {
    val pointsWithIndex = points.zipWithIndex.init
    val pivot = pointsWithIndex.zip(pointsWithIndex.drop(1)).map(x => {
      val point1 = x._1._1
      val point2 = x._2._1
      val vector1 = point1 - point2
      val distance = PointDistance.eval(vector1)
      (x._2, distance)
    }).sortBy(-_._2).map(_._1).take(count)

    pivot.sortBy(_._2)
  }

  private def calcCells(): Array[(Rectangle, Int)] = {
    val dimension = numDimensions
    val cellSize = ConfigConstants.LOCAL_CELL_SIZE
    val cells = ArrayBuffer.empty[ArrayBuffer[Point]]
    cells.append(ArrayBuffer(points.head))
    var findCells = true
    for (point <- points.tail) {
      if (cells.length <= points.length * ConfigConstants.LOCAL_CELL_THRESHOLD) {
        val cell = cells.find(x => x.head.approxMinDist(point) <= cellSize)
        if (cell.isDefined) {
          cell.get.append(point)
        } else {
          cells.append(ArrayBuffer(point))
        }
      } else {
        findCells = false
      }
    }

    if (findCells) {
      cells.toArray.map(points => {
        val min = (0 until dimension).map(i => points.map(_.coord(i)).min).toArray
        val max = (0 until dimension).map(i => points.map(_.coord(i)).max).toArray
        (Rectangle(Point(min), Point(max)), points.length)
      })
    } else {
      Array.empty[(Rectangle, Int)]
    }
  }

  def refresh(threshold: Double): Unit = {
    extendedMBR = calcExtendedMBR(threshold)
  }

  def getMBR: Rectangle = mbr

  def getExtendedMBR: Rectangle = extendedMBR

  def getGlobalIndexedPivot: Array[Point] = globalIndexedPivot

  def getLocalIndexedPivot: Array[Point] = localIndexedPivot

  def getCells: Array[(Rectangle, Int)] = cells
}