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

package org.apache.spark.sql.execution.dita

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, LessThanOrEqual, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.dita.{TrajectorySimilarityExpression, TrajectorySimilarityFunction}
import org.apache.spark.sql.catalyst.plans.{logical, JoinType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * A pattern that finds joins with trajectory similarity conditions.
  */
object ExtractTrajectorySimilarityJoin extends Logging with PredicateHelper {
  type ReturnType =
    (JoinType, Expression, Expression, TrajectorySimilarityFunction, Literal, LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case logical.Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      if (condition.isDefined) {
        condition.get match {
          case LessThanOrEqual(TrajectorySimilarityExpression(function, traj1, traj2), threshold) =>
            Some((joinType, traj1, traj2, function, threshold.asInstanceOf[Literal], left, right))
          case _ => None
        }
      } else {
        None
      }
    case _ => None
  }
}
