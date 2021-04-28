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
package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}

/**
 * This is a helper class that wraps the methods in KinesisUtils into more Python-friendly class and
 * function so that it can be easily instantiated and called from Python's KinesisUtils.
 */
private class KinesisUtilsPythonHelper {

  // scalastyle:off
  def createStream(
      jssc: JavaStreamingContext,
      kinesisAppName: String,
      streamName: String,
      endpointUrl: String,
      regionName: String,
      initialPositionInStream: Int,
      checkpointInterval: Duration,
      storageLevel: StorageLevel,
      awsAccessKeyId: String,
      awsSecretKey: String,
      stsAssumeRoleArnKinesis: String,
      stsSessionNameKinesis: String,
      stsExternalIdKinesis: String,
      stsAssumeRoleArnDynamoDB: String,
      stsSessionNameDynamoDB: String,
      stsExternalIdDynamoDB: String,
      stsAssumeRoleArnCloudWatch: String,
      stsSessionNameCloudWatch: String,
      stsExternalIdCloudWatch: String): JavaReceiverInputDStream[Array[Byte]] = {
    // scalastyle:on
    if (!(stsAssumeRoleArnKinesis != null && stsSessionNameKinesis != null && stsExternalIdKinesis != null)
        && !(stsAssumeRoleArnKinesis == null && stsSessionNameKinesis == null && stsExternalIdKinesis == null)) {
      throw new IllegalArgumentException("stsAssumeRoleArnKinesis, stsSessionNameKinesis, and stsExternalIdKinesis " +
        "must all be defined or all be null")
    }
    if (!(stsAssumeRoleArnDynamoDB != null && stsSessionNameDynamoDB != null && stsExternalIdDynamoDB != null)
        && !(stsAssumeRoleArnDynamoDB == null && stsSessionNameDynamoDB == null && stsExternalIdDynamoDB == null)) {
      throw new IllegalArgumentException("stsAssumeRoleArnDynamoDB, stsSessionNameDynamoDB, and stsExternalIdDynamoDB " +
        "must all be defined or all be null")
    }
    if (!(stsAssumeRoleArnDynamoDB != null && stsSessionNameCloudWatch != null && stsExternalIdCloudWatch != null)
        && !(stsAssumeRoleArnDynamoDB == null && stsSessionNameCloudWatch == null && stsExternalIdCloudWatch == null)) {
      throw new IllegalArgumentException("stsAssumeRoleArnDynamoDB, stsSessionNameCloudWatch, and stsExternalIdCloudWatch " +
        "must all be defined or all be null")
    }
    if (awsAccessKeyId == null && awsSecretKey != null) {
      throw new IllegalArgumentException("awsSecretKey is set but awsAccessKeyId is null")
    }
    if (awsAccessKeyId != null && awsSecretKey == null) {
      throw new IllegalArgumentException("awsAccessKeyId is set but awsSecretKey is null")
    }

    val kinesisInitialPosition = initialPositionInStream match {
      case 0 => InitialPositionInStream.LATEST
      case 1 => InitialPositionInStream.TRIM_HORIZON
      case _ => throw new IllegalArgumentException(
        "Illegal InitialPositionInStream. Please use " +
          "InitialPositionInStream.LATEST or InitialPositionInStream.TRIM_HORIZON")
    }

    val builder = KinesisInputDStream.builder.
      streamingContext(jssc).
      checkpointAppName(kinesisAppName).
      streamName(streamName).
      endpointUrl(endpointUrl).
      regionName(regionName).
      initialPosition(KinesisInitialPositions.fromKinesisInitialPosition(kinesisInitialPosition)).
      checkpointInterval(checkpointInterval).
      storageLevel(storageLevel)

    if (stsAssumeRoleArnKinesis != null && stsSessionNameKinesis != null && stsExternalIdKinesis != null) {
      val kinesisCredsProvider = STSCredentials(
        stsAssumeRoleArnKinesis, stsSessionNameKinesis, Option(stsExternalIdKinesis),
        BasicCredentials(awsAccessKeyId, awsSecretKey))
      val dynamoDBCredsProvider = STSCredentials(
        stsAssumeRoleArnDynamoDB, stsSessionNameDynamoDB, Option(stsExternalIdDynamoDB),
        BasicCredentials(awsAccessKeyId, awsSecretKey))
      val cloudWatchCredsProvider = STSCredentials(
        stsAssumeRoleArnCloudWatch, stsSessionNameCloudWatch, Option(stsExternalIdCloudWatch),
        BasicCredentials(awsAccessKeyId, awsSecretKey))
      builder.
        kinesisCredentials(kinesisCredsProvider).
        dynamoDBCredentials(dynamoDBCredsProvider).
        cloudWatchCredentials(cloudWatchCredsProvider).
        buildWithMessageHandler(KinesisInputDStream.defaultMessageHandler)
    } else {
      if (awsAccessKeyId == null && awsSecretKey == null) {
        builder.build()
      } else {
        builder.kinesisCredentials(BasicCredentials(awsAccessKeyId, awsSecretKey)).build()
      }
    }
  }
}
