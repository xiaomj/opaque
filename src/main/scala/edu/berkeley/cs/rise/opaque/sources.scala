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

package edu.berkeley.cs.rise.opaque

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

import java.util.Base64

import edu.berkeley.cs.rise.opaque.execution.Block
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec

class EncryptedSource extends SchemaRelationProvider with CreatableRelationProvider {
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = {
    EncryptedScan(parameters("path"), schema, isOblivious(parameters))(
      sqlContext.sparkSession)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val blocks: RDD[Block] = data.queryExecution.executedPlan.asInstanceOf[OpaqueOperatorExec]
      .executeBlocked()
    blocks.map(x => Base64.getEncoder.encodeToString(x.bytes)).saveAsTextFile(parameters("path"))
//    blocks.map(block => (0, block.bytes)).saveAsSequenceFile(parameters("path"))
    EncryptedScan(parameters("path"), data.schema, isOblivious(parameters))(
      sqlContext.sparkSession)
  }

  private def isOblivious(parameters: Map[String, String]): Boolean = {
    parameters.get("oblivious") match {
      case Some("true") => true
      case _ => false
    }
  }
}

case class EncryptedScan(
    path: String,
    override val schema: StructType,
    val isOblivious: Boolean)(
    @transient val sparkSession: SparkSession)
  extends BaseRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def needConversion: Boolean = false

  //  def buildBlockedScan(): RDD[Block] = sparkSession.sparkContext
//    .sequenceFile[Int, Array[Byte]](path).map {
//      case (_, bytes) => Block(bytes)
//    }

  def buildBlockedScan(): RDD[Block] = sparkSession.sparkContext.textFile(path).map(x => Block(Base64.getDecoder().decode(x))
}
