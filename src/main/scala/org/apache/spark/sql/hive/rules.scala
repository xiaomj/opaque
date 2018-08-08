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

package org.apache.spark.sql.hive.rules

import edu.berkeley.cs.rise.opaque.EncryptedScan
import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec
import org.apache.spark.sql.InMemoryRelationMatcher
import org.apache.spark.sql.UndoCollapseProject
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.MetastoreRelation


object EncryptLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // 读取内存明文
    case Encrypt(isOblivious, LocalRelation(output, data)) =>
      EncryptedLocalRelation(output, data, isOblivious)
  }
}

object ConvertToOpaqueOperators extends Rule[LogicalPlan] {
  def isOblivious(plan: LogicalPlan): Boolean = {
    isEncrypted(plan) && plan.find {
      case p: OpaqueOperator => p.isOblivious
      case _ => false
    }.nonEmpty
  }

  def isOblivious(plan: SparkPlan): Boolean = {
    isEncrypted(plan) && plan.find {
      case p: OpaqueOperatorExec => p.isOblivious
      case _ => false
    }.nonEmpty
  }

  def isEncrypted(plan: LogicalPlan): Boolean = {
    true
  }

  def isEncrypted(plan: SparkPlan): Boolean = {
    true
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // 读取外部加密文件
    case l @ LogicalRelation(baseRelation: EncryptedScan, _, _) =>
      EncryptedBlockRDD(l.output, baseRelation.buildBlockedScan(), baseRelation.isOblivious)

    case l: MetastoreRelation => {
      println("find metastore relation")
      l
    }

    case p @ Project(projectList, child) if isEncrypted(child) =>
      EncryptedProject(projectList, child)

    // We don't support null values yet, so there's no point in checking whether the output of an
    // encrypted operator is null
    case p @ Filter(And(IsNotNull(_), IsNotNull(_)), child) if isEncrypted(child) =>
      child
    case p @ Filter(IsNotNull(_), child) if isEncrypted(child) =>
      child
    case p @ Filter(condition, child) if isEncrypted(child) =>
      EncryptedFilter(condition, child)
    case p @ Sort(order, true, child) if isEncrypted(child) =>
      EncryptedSort(order, child)

    case p @ Join(left, right, joinType, condition) if isEncrypted(p) =>
      EncryptedJoin(
        left, right, joinType, condition)

    case p @ Aggregate(groupingExprs, aggExprs, child) if isEncrypted(p) =>
      UndoCollapseProject.separateProjectAndAgg(p) match {
        case Some((projectExprs, aggExprs)) =>
          EncryptedProject(
            projectExprs,
            EncryptedAggregate(
              groupingExprs, aggExprs,
              EncryptedSort(
                groupingExprs.map(e => SortOrder(e, Ascending)),
                child)))
        case None =>
          EncryptedAggregate(
            groupingExprs, aggExprs,
            EncryptedSort(
              groupingExprs.map(e => SortOrder(e, Ascending)),
              child))
      }

    // For now, just ignore limits. TODO: Implement Opaque operators for these
    case p @ GlobalLimit(_, child) if isEncrypted(p) => child
    case p @ LocalLimit(_, child) if isEncrypted(p) => child

    case p @ Union(Seq(left, right)) if isEncrypted(p) =>
      ObliviousUnion(left, right)

    case InMemoryRelationMatcher(output, storageLevel, child) if isEncrypted(child) =>
      EncryptedBlockRDD(
        output,
        Utils.ensureCached(
          child.asInstanceOf[OpaqueOperatorExec].executeBlocked(),
          storageLevel),
        isOblivious(child))
  }
}
