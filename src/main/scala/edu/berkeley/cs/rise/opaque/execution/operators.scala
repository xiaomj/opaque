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

import java.util.Base64

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import scala.collection.mutable.ArrayBuffer


trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}

/**
  * API创建的表，手动加密
  * @param output
  * @param plaintextData
  */
case class EncryptLocalRelationExec(
    output: Seq[Attribute],
    plaintextData: Seq[InternalRow])
  extends LeafExecNode with OpaqueOperatorExec {

  private val unsafeRows: Array[InternalRow] = {
    val proj = UnsafeProjection.create(output, output)
    val result: Array[InternalRow] = plaintextData.map(r => proj(r).copy()).toArray
    result
  }

  override def executeBlocked(): RDD[Block] = {
    // Locally partition plaintextData using the same logic as ParallelCollectionRDD.slice
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until length.toInt).iterator.map { i => (i, i+1) }
    }

    val slicedPlaintextData: Seq[Seq[InternalRow]] =
      positions(unsafeRows.length, sqlContext.sparkContext.defaultParallelism).map {
        case (start, end) => unsafeRows.slice(start, end).toSeq
      }.toSeq

    // Encrypt each local partition
    val encryptedPartitions: Seq[Block] =
      slicedPlaintextData.map(slice =>
        Utils.encryptInternalRowsFlatbuffers(slice, output.map(_.dataType)))

    // Make an RDD from the encrypted partitions
    sqlContext.sparkContext.parallelize(encryptedPartitions)
  }
}

/**
  * 加密好的磁盘文件，直接加载
  * @param output
  * @param rdd
  */
case class EncryptLogicalRelationExec(
    output: Seq[Attribute],
    rdd: RDD[Block])
  extends LeafExecNode with OpaqueOperatorExec {

  override def executeBlocked(): RDD[Block] = rdd
}

case class Block(bytes: Array[Byte]) extends Serializable

trait OpaqueOperatorExec extends SparkPlan {
  def executeBlocked(): RDD[Block]

  def executeChild(child: SparkPlan) = child match {
    case child :OpaqueOperatorExec =>
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    case _ =>
      // 默认用第一列存放加密密文
      child.execute().map {
        x => Block(Base64.getDecoder().decode(x.getUTF8String(0).toString))
      }
  }

  def timeOperator[A](childRDD: RDD[A], desc: String)(f: RDD[A] => RDD[Block]): RDD[Block] = {
    import Utils.time
    Utils.ensureCached(childRDD)
    time(s"Force child of $desc") { childRDD.count }
    time(desc) {
      val result = f(childRDD)
      Utils.ensureCached(result)
      result.count
      result
    }
  }

  /**
   * An Opaque operator cannot return plaintext rows, so this method should normally not be invoked.
   * Instead use executeBlocked, which returns the data as encrypted blocks.
   *
   * However, when encrypted data is cached, Spark SQL's InMemoryRelation attempts to call this
   * method and persist the resulting RDD. [[ConvertToOpaqueOperators]] later eliminates the dummy
   * relation from the logical plan, but this only happens after InMemoryRelation has called this
   * method. We therefore have to silently return an empty RDD here.
   */
  override def doExecute() = {
    sqlContext.sparkContext.emptyRDD
    // throw new UnsupportedOperationException("use executeBlocked")
  }

  override def executeCollect(): Array[InternalRow] = {
    executeBlocked().collect().flatMap { block =>
      Utils.decryptBlockFlatbuffers(block)
    }
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = executeBlocked()

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Block]) => if (it.hasNext) Some(it.next()) else None, p)

      res.foreach {
        case Some(block) =>
          buf ++= Utils.decryptBlockFlatbuffers(block)
        case None => {}
      }

      partsScanned += p.size
    }

    if (buf.size > n) {
      buf.take(n).toArray
    } else {
      buf.toArray
    }
  }
}

case class EncryptSortExec(order: Seq[SortOrder], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    val orderSer = Utils.serializeSortOrder(order, child.output)
    EncryptSortExec.sort(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), orderSer)
  }
}

object EncryptSortExec {
  import Utils.time

  def sort(childRDD: RDD[Block], orderSer: Array[Byte]): RDD[Block] = {
    Utils.ensureCached(childRDD)
    time("force child of EncryptedSort") { childRDD.count }
    // RA.initRA(childRDD)

    time("non-oblivious sort") {
      val numPartitions = childRDD.partitions.length
      val result =
        if (numPartitions <= 1) {
          childRDD.map { block =>
            val (enclave, eid) = Utils.initEnclave()
            val sortedRows = enclave.ExternalSort(eid, orderSer, block.bytes)
            Block(sortedRows)
          }
        } else {
          // Collect a sample of the input rows
          val sampled = time("non-oblivious sort - Sample") {
            Utils.concatEncryptedBlocks(childRDD.map { block =>
              val (enclave, eid) = Utils.initEnclave()
              val sampledBlock = enclave.Sample(eid, block.bytes)
              Block(sampledBlock)
            }.collect)
          }
          // Find range boundaries locally
          val (enclave, eid) = Utils.initEnclave()
          val boundaries = time("non-oblivious sort - FindRangeBounds") {
            enclave.FindRangeBounds(eid, orderSer, numPartitions, sampled.bytes)
          }
          // Broadcast the range boundaries and use them to partition the input
          childRDD.flatMap { block =>
            val (enclave, eid) = Utils.initEnclave()
            val partitions = enclave.PartitionForSort(
              eid, orderSer, numPartitions, block.bytes, boundaries)
            partitions.zipWithIndex.map {
              case (partition, i) => (i, Block(partition))
            }
          }
            // Shuffle the input to achieve range partitioning and sort locally
            .groupByKey(numPartitions).map {
            case (i, blocks) =>
              val (enclave, eid) = Utils.initEnclave()
              Block(enclave.ExternalSort(
                eid, orderSer, Utils.concatEncryptedBlocks(blocks.toSeq).bytes))
          }
        }
      Utils.ensureCached(result)
      result.count()
      result
    }
  }
}

case class ObliviousProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def executeBlocked() = {
    val projectListSer = Utils.serializeProjectList(projectList, child.output)
    timeOperator(executeChild(child), "ObliviousProjectExec") {
      childRDD => childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Project(eid, projectListSer, block.bytes))
      }
    }
  }
}


case class ObliviousFilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    val conditionSer = Utils.serializeFilterExpression(condition, child.output)

    timeOperator(executeChild(child), "ObliviousFilterExec") {
      childRDD => childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Filter(eid, conditionSer, block.bytes))
      }
    }
  }
}

case class EncryptAggregateExec(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)

  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)

  override def executeBlocked(): RDD[Block] = {
    val aggExprSer = Utils.serializeAggOp(groupingExpressions, aggExpressions, child.output)

    timeOperator(executeChild(child), "EncryptedAggregateExec") { childRDD =>
      val (firstRows, lastGroups, lastRows) = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        val (firstRow, lastGroup, lastRow) = enclave.NonObliviousAggregateStep1(
          eid, aggExprSer, block.bytes)
        (Block(firstRow), Block(lastGroup), Block(lastRow))
      }.collect.unzip3

      // Send first row to previous partition and last group to next partition
      val shiftedFirstRows = firstRows.drop(1) :+ Utils.emptyBlock
      val shiftedLastGroups = Utils.emptyBlock +: lastGroups.dropRight(1)
      val shiftedLastRows = Utils.emptyBlock +: lastRows.dropRight(1)
      val shifted = (shiftedFirstRows, shiftedLastGroups, shiftedLastRows).zipped.toSeq
      assert(shifted.size == childRDD.partitions.length)
      val shiftedRDD = sparkContext.parallelize(shifted, childRDD.partitions.length)

      childRDD.zipPartitions(shiftedRDD) { (blockIter, boundaryIter) =>
        (blockIter.toSeq, boundaryIter.toSeq) match {
          case (Seq(block), Seq(Tuple3(
            nextPartitionFirstRow, prevPartitionLastGroup, prevPartitionLastRow))) =>
            val (enclave, eid) = Utils.initEnclave()
            Iterator(Block(enclave.NonObliviousAggregateStep2(
              eid, aggExprSer, block.bytes,
              nextPartitionFirstRow.bytes, prevPartitionLastGroup.bytes,
              prevPartitionLastRow.bytes)))
        }
      }
    }
  }
}

case class EncryptSortMergeJoinExec(
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSchema: Seq[Attribute],
    rightSchema: Seq[Attribute],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def executeBlocked() = {
    val joinExprSer = Utils.serializeJoinExpression(
      joinType, leftKeys, rightKeys, leftSchema, rightSchema)

    timeOperator(executeChild(child), "EncryptedSortMergeJoinExec") { childRDD =>
      val lastPrimaryRows = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.ScanCollectLastPrimary(eid, joinExprSer, block.bytes))
      }.collect
      val shifted = Utils.emptyBlock +: lastPrimaryRows.dropRight(1)
      assert(shifted.size == childRDD.partitions.length)
      val processedJoinRowsRDD =
        sparkContext.parallelize(shifted, childRDD.partitions.length)

      childRDD.zipPartitions(processedJoinRowsRDD) { (blockIter, joinRowIter) =>
        (blockIter.toSeq, joinRowIter.toSeq) match {
          case (Seq(block), Seq(joinRow)) =>
            val (enclave, eid) = Utils.initEnclave()
            Iterator(Block(enclave.NonObliviousSortMergeJoin(
              eid, joinExprSer, block.bytes, joinRow.bytes)))
        }
      }
    }
  }
}

case class ObliviousUnionExec(
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode with OpaqueOperatorExec {
  import Utils.time

  override def output: Seq[Attribute] =
    left.output

  override def executeBlocked() = {
    val leftRDD = execute(left)
    val rightRDD = execute(right)
    Utils.ensureCached(leftRDD)
    time("Force left child of ObliviousUnionExec") { leftRDD.count }
    Utils.ensureCached(rightRDD)
    time("Force right child of ObliviousUnionExec") { rightRDD.count }

    // RA.initRA(leftRDD)

    val unioned = leftRDD.zipPartitions(rightRDD) { (leftBlockIter, rightBlockIter) =>
      (leftBlockIter.toSeq ++ rightBlockIter.toSeq) match {
        case Seq(leftBlock, rightBlock) =>
          Iterator(Utils.concatEncryptedBlocks(Seq(leftBlock, rightBlock)))
        case Seq(block) => Iterator(block)
        case Seq() => Iterator.empty
      }
    }
    Utils.ensureCached(unioned)
    time("ObliviousUnionExec") { unioned.count }
    unioned
  }
}
