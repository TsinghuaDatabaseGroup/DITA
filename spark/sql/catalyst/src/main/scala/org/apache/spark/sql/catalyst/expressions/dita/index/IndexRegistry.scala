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

package org.apache.spark.sql.catalyst.expressions.dita.index

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable.ArrayBuffer

case class IndexEntry(indexName: String, tableName: Option[String], plan: LogicalPlan,
                      relation: IndexedRelation)

class IndexRegistry {
  protected val indexEntries = new ArrayBuffer[IndexEntry]

  def registerIndex(indexName: String, tableName: Option[String], plan: LogicalPlan,
                    relation: IndexedRelation): Unit = synchronized {
    indexEntries.append(IndexEntry(indexName, tableName, plan, relation))
  }

  def lookupIndex(plan: LogicalPlan): Option[IndexEntry] = synchronized {
    indexEntries.find(entry => entry.plan.sameResult(plan))
  }

  def lookupIndex(indexName: String, plan: LogicalPlan): Option[IndexEntry] = synchronized {
    indexEntries.find(entry => entry.indexName.equals(indexName) && entry.plan.sameResult(plan))
  }

  def dropIndex(plan: LogicalPlan): Boolean = synchronized {
    val index = indexEntries.zipWithIndex.find(entry => entry._1.plan.sameResult(plan)).map(_._2)
    if (index.isDefined) {
      indexEntries.remove(index.get)
      true
    } else {
      false
    }
  }

  def dropIndex(indexName: String, tableName: Option[String]): Boolean = synchronized {
    val index = indexEntries.zipWithIndex.find(entry => entry._1.indexName.equals(indexName)
      && entry._1.tableName.equals(tableName)).map(_._2)
    if (index.isDefined) {
      indexEntries.remove(index.get)
      true
    } else {
      false
    }
  }

  def showIndexes(): Seq[String] = synchronized {
    indexEntries.map(entry => s"(${entry.tableName}, ${entry.indexName})")
  }

  def clear(): Unit = synchronized {
    indexEntries.clear()
  }

  override def clone(): IndexRegistry = synchronized {
    val registry = new IndexRegistry
    indexEntries.iterator.foreach(entry =>
      registry.registerIndex(entry.indexName, entry.tableName, entry.plan, entry.relation))
    registry
  }
}
