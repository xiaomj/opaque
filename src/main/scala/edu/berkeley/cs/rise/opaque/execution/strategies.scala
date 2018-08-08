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

package edu.berkeley.cs.rise.opaque.execution

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import edu.berkeley.cs.rise.opaque.logical._

object EncryptStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case EncryptProject(projectList, child) =>
      ObliviousProjectExec(projectList, planLater(child)) :: Nil

    case EncryptFilter(condition, child) =>
      ObliviousFilterExec(condition, planLater(child)) :: Nil

    case EncryptSort(order, child) =>
      EncryptSortExec(order, planLater(child)) :: Nil

    case EncryptJoin(left, right, joinType, condition) =>
      Join(left, right, joinType, condition) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          val (leftProjSchema, leftKeysProj, tag) = tagForJoin(leftKeys, left.output, true)
          val (rightProjSchema, rightKeysProj, _) = tagForJoin(rightKeys, right.output, false)
          val leftProj = ObliviousProjectExec(leftProjSchema, planLater(left))
          val rightProj = ObliviousProjectExec(rightProjSchema, planLater(right))
          val unioned = ObliviousUnionExec(leftProj, rightProj)
          val sorted = EncryptedSortExec(sortForJoin(leftKeysProj, tag, unioned.output), unioned)
          val joined = EncryptedSortMergeJoinExec(
            joinType,
            leftKeysProj,
            rightKeysProj,
            leftProjSchema.map(_.toAttribute),
            rightProjSchema.map(_.toAttribute),
            (leftProjSchema ++ rightProjSchema).map(_.toAttribute),
            sorted)
          ObliviousProjectExec(dropTags(left.output, right.output), joined) :: Nil
        case _ => Nil
      }

    case a @ EncryptAggregate(groupingExpressions, aggExpressions, child) =>
      EncryptAggregateExec(groupingExpressions, aggExpressions, planLater(child)) :: Nil

    case ObliviousUnion(left, right) =>
      ObliviousUnionExec(planLater(left), planLater(right)) :: Nil

    case Encrypt(child) =>
      EncryptExec(planLater(child)) :: Nil

    case EncryptLocalRelation(output, plaintextData) =>
      EncryptLocalTableScanExec(output, plaintextData) :: Nil

    case EncryptLogicalRelation(output, rdd) =>
      EncryptLogicalRelationExec(output, rdd) :: Nil

    case _ => Nil
  }

  private def tagForJoin(
      keys: Seq[Expression], input: Seq[Attribute], isLeft: Boolean)
    : (Seq[NamedExpression], Seq[NamedExpression], NamedExpression) = {
    val keysProj = keys.zipWithIndex.map { case (k, i) => Alias(k, "_" + i)() }
    val tag = Alias(Literal(if (isLeft) 0 else 1), "_tag")()
    (Seq(tag) ++ keysProj ++ input, keysProj.map(_.toAttribute), tag.toAttribute)
  }

  private def sortForJoin(
      leftKeys: Seq[Expression], tag: Expression, input: Seq[Attribute]): Seq[SortOrder] =
    leftKeys.map(k => SortOrder(k, Ascending)) :+ SortOrder(tag, Ascending)

  private def dropTags(
      leftOutput: Seq[Attribute], rightOutput: Seq[Attribute]): Seq[NamedExpression] =
    leftOutput ++ rightOutput
}