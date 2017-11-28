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

package org.apache.spark.sql.execution.dita.index.local

import org.apache.spark.sql.catalyst.expressions.dita.common.shape.{Point, Rectangle, Shape}
import org.apache.spark.sql.catalyst.expressions.dita.index.LocalIndex

import scala.collection.mutable

// Reference: https://github.com/InitialDLab/Simba/blob/master/src/main/scala/org/apache/spark/sql/simba/index/RTree.scala

case class RTreeNode(m_mbr: Rectangle, m_child: Array[RTreeEntry], isLeaf: Boolean) {
  def this(m_mbr: Rectangle, children: Array[(Rectangle, RTreeNode)]) = {
    this(m_mbr, children.map(x => RTreeInternalEntry(x._1, x._2)), false)
  }

  def this(m_mbr: Rectangle, children: => Array[(Point, Int)]) = {
    this(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, 1)), true)
  }

  def this(m_mbr: Rectangle, children: Array[(Rectangle, Int, Int)]) = {
    this(m_mbr, children.map(x => RTreeLeafEntry(x._1, x._2, x._3)), true)
  }

  val size: Long = {
    if (isLeaf) m_child.map(x => x.asInstanceOf[RTreeLeafEntry].size).sum
    else m_child.map(x => x.asInstanceOf[RTreeInternalEntry].node.size).sum
  }
}

abstract class RTreeEntry extends Shape {
  def minDist(x: Shape): Double

  def intersects(x: Shape): Boolean
}

case class RTreeLeafEntry(shape: Shape, m_data: Int, size: Int) extends RTreeEntry {
  override def minDist(x: Shape): Double = shape.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(shape)
}

case class RTreeInternalEntry(mbr: Rectangle, node: RTreeNode) extends RTreeEntry {
  override def minDist(x: Shape): Double = mbr.minDist(x)
  override def intersects(x: Shape): Boolean = x.intersects(mbr)
}

case class RTree(root: RTreeNode) extends LocalIndex with Serializable {
  def range(query: Rectangle): Array[(Shape, Int)] = {
    val ans = mutable.ArrayBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.intersects(query) && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case RTreeInternalEntry(mbr, node) =>
            if (query.intersects(mbr)) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, _) =>
            if (query.intersects(shape)) ans += ((shape, m_data))
        }
      }
    }
    ans.toArray
  }

  def circleRange(origin: Shape, r: Double): List[(Shape, Int)] = {
    val ans = mutable.ListBuffer[(Shape, Int)]()
    val st = new mutable.Stack[RTreeNode]()
    if (root.m_mbr.minDist(origin) <= r && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach{
          case RTreeInternalEntry(mbr, node) =>
            if (origin.minDist(mbr) <= r) st.push(node)
        }
      } else {
        now.m_child.foreach {
          case RTreeLeafEntry(shape, m_data, _) =>
            if (origin.minDist(shape) <= r) ans += ((shape, m_data))
        }
      }
    }
    ans.toList
  }
}

object RTree {
  def apply(entries: Array[(Point, Int)], max_entries_per_node: Int): RTree = {
    val dimension = entries(0)._1.coord.length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def recursiveGroupPoint(entries: Array[(Point, Int)],
                            cur_dim: Int, until_dim: Int): Array[Array[(Point, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(_._1.coord(cur_dim) < _._1.coord(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupPoint(now, cur_dim + 1, until_dim))
      } else grouped
    }

    val grouped = recursiveGroupPoint(entries, 0, dimension - 1)
    val rtree_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(_ => Double.MaxValue)
      val max = new Array[Double](dimension).map(_ => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.coord(i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.coord(i))
      })
      val mbr = Rectangle(Point(min), Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim: Array[Int]): Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left: (Rectangle, RTreeNode), right: (Rectangle, RTreeNode)): Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupRTreeNode(entries: Array[(Rectangle, RTreeNode)], cur_dim: Int, until_dim: Int)
    : Array[Array[(Rectangle, RTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupRTreeNode(now, cur_dim + 1, until_dim))
      } else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = Rectangle(Point(min), Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(_ => Double.MaxValue)
    val max = new Array[Double](dimension).map(_ => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
    })

    val mbr = Rectangle(Point(min), Point(max))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }

  def apply(entries: Array[(Rectangle, Int, Int)], max_entries_per_node: Int): RTree = {
    val dimension = entries(0)._1.low.coord.length
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def compMBR(dim: Int)(left: (Rectangle, Int, Int), right: (Rectangle, Int, Int)): Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupMBR(entries: Array[(Rectangle, Int, Int)], cur_dim: Int, until_dim: Int)
    : Array[Array[(Rectangle, Int, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(compMBR(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupMBR(now, cur_dim + 1, until_dim))
      } else grouped
    }

    val grouped = recursiveGroupMBR(entries, 0, dimension - 1)
    val rtree_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension).map(_ => Double.MaxValue)
      val max = new Array[Double](dimension).map(_ => Double.MinValue)
      list.foreach(now => {
        for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
        for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
      })
      val mbr = Rectangle(Point(min), Point(max))
      rtree_nodes += ((mbr, new RTreeNode(mbr, list)))
    })

    var cur_rtree_nodes = rtree_nodes.toArray
    var cur_len = cur_rtree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
      remaining /= dim(i)
    }

    def over(dim: Array[Int]): Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left: (Rectangle, RTreeNode), right: (Rectangle, RTreeNode)): Boolean = {
      val left_center = left._1.low.coord(dim) + left._1.high.coord(dim)
      val right_center = right._1.low.coord(dim) + right._1.high.coord(dim)
      left_center < right_center
    }

    def recursiveGroupRTreeNode(entries: Array[(Rectangle, RTreeNode)],
                                cur_dim: Int, until_dim: Int): Array[Array[(Rectangle, RTreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => {
          recursiveGroupRTreeNode(now, cur_dim + 1, until_dim)
        })
      } else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupRTreeNode(cur_rtree_nodes, 0, dimension - 1)
      var tmp_nodes = mutable.ArrayBuffer[(Rectangle, RTreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension).map(_ => Double.MaxValue)
        val max = new Array[Double](dimension).map(_ => Double.MinValue)
        list.foreach(now => {
          for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
          for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
        })
        val mbr = Rectangle(Point(min), Point(max))
        tmp_nodes += ((mbr, new RTreeNode(mbr, list)))
      })
      cur_rtree_nodes = tmp_nodes.toArray
      cur_len = cur_rtree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension).map(_ => Double.MaxValue)
    val max = new Array[Double](dimension).map(_ => Double.MinValue)
    cur_rtree_nodes.foreach(now => {
      for (i <- 0 until dimension) min(i) = Math.min(min(i), now._1.low.coord(i))
      for (i <- 0 until dimension) max(i) = Math.max(max(i), now._1.high.coord(i))
    })

    val mbr = Rectangle(Point(min), Point(max))
    val root = new RTreeNode(mbr, cur_rtree_nodes)
    new RTree(root)
  }
}