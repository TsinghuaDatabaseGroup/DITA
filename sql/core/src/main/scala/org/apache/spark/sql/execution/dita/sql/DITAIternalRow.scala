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

package org.apache.spark.sql.execution.dita.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory

class DITAIternalRow(internalRow: InternalRow, points: Array[Point]) extends Trajectory(points) {
  var row: InternalRow = internalRow
}