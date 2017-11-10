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

package org.apache.spark.sql.catalyst.expressions.dita.common.shape

case class Point(coord: Array[Double]) extends Shape {
  def this() = this(Array())

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: Rectangle => mbr.minDist(this)
      case r: SimpleRange => r.minDist(this)
    }
  }

  def minDist(other: Point): Double = {
    require(coord.length == other.coord.length)
    var ans = 0.0
    for (i <- coord.indices)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }

  override def approxMinDist(other: Shape): Double = {
    other match {
      case p: Point => approxMinDist(p)
      case mbr: Rectangle => mbr.approxMinDist(this)
    }
  }

  def approxMinDist(other: Point): Double = {
    var ans = 0.0
    for (i <- coord.indices)
      ans = math.max(ans, math.abs(coord(i) - other.coord(i)))
    ans
  }

  override def intersects(other: Shape): Boolean = {
    other match {
      case p: Point => p == this
      case mbr: Rectangle => mbr.contains(this)
      case r: SimpleRange => r.intersects(this)
    }
  }

  def ==(other: Point): Boolean = other match {
    case p: Point =>
      if (p.coord.length != coord.length) false
      else {
        for (i <- coord.indices)
          if (coord(i) != p.coord(i)) return false
        true
      }
    case _ => false
  }

  def <=(other: Point): Boolean = {
    for (i <- coord.indices)
      if (coord(i) > other.coord(i)) return false
    true
  }

  def -(other: Point): Point = Point(coord.zip(other.coord).map(x => x._1 - x._2))

  def shift(d: Double): Point = Point(coord.map(x => x + d))

  override def toString: String = {
    var s = "POINT("
    s += coord(0).toString
    for (i <- 1 until coord.length) s += "," + coord(i)
    s + ")"
  }
}
