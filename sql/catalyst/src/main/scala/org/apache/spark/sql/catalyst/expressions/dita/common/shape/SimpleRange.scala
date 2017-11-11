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

case class SimpleRange(dimensionIndex: Int, low: Double, high: Double) extends Shape {
  require(low <= high, s"$low, $high")

  def minDist(other: Shape): Double = {
    other match {
      case p: Point =>
        if (p.coord(dimensionIndex) < low) {
          low - p.coord(dimensionIndex)
        } else if (p.coord(dimensionIndex) > high) {
          p.coord(dimensionIndex) - high
        } else {
          math.min(p.coord(dimensionIndex) - low, high - p.coord(dimensionIndex))
        }
      case mbr: Rectangle =>
        if (mbr.high.coord(dimensionIndex) < low) {
          low - mbr.high.coord(dimensionIndex)
        } else if (high < mbr.low.coord(dimensionIndex)) {
          mbr.low.coord(dimensionIndex) - high
        } else {
          0.0
        }
    }
  }

  def intersects(other: Shape): Boolean = {
    other match {
      case p: Point => low <= p.coord(dimensionIndex) && p.coord(dimensionIndex) <= high
      case mbr: Rectangle => mbr.high.coord(dimensionIndex) >= low &&
        high >= mbr.low.coord(dimensionIndex)
    }
  }
}
