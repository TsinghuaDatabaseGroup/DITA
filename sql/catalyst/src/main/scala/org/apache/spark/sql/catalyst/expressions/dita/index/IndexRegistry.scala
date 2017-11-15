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

case class IndexEntry(name: String, plan: LogicalPlan, relation: IndexedRelation)

class IndexRegistry {
  protected val indexEntries = new ArrayBuffer[IndexEntry]

  def registerIndex(name: String, plan: LogicalPlan, relation: IndexedRelation):
  Unit = synchronized {
    indexEntries.append(IndexEntry(name, plan, relation))
  }

  def lookupIndex(plan: LogicalPlan): Option[IndexEntry] = synchronized {
    indexEntries.find(entry => entry.plan.sameResult(plan))
  }

  def lookupIndex(name: String, plan: LogicalPlan): Option[IndexEntry] = synchronized {
    indexEntries.find(entry => entry.name.equals(name) && entry.plan.sameResult(plan))
  }

  def clear(): Unit = synchronized {
    indexEntries.clear()
  }

  override def clone(): IndexRegistry = synchronized {
    val registry = new IndexRegistry
    indexEntries.iterator.foreach(entry =>
      registry.registerIndex(entry.name, entry.plan, entry.relation))
    registry
  }
}
