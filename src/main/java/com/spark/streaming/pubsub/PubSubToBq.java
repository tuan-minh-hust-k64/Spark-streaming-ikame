/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spark.streaming.pubsub;

import com.google.cloud.bigquery.storage.v1.*;
import com.spark.utils.BatchStreamBq;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.json.JSONArray;
import org.json.JSONObject;

public class PubSubToBq {
  public static void main(String[] args) throws InterruptedException {
    JavaStreamingContext jsc;

    SparkConf sparkConf = new SparkConf().setAppName("PubSubToBigQuery Dataproc Job");
    jsc = new JavaStreamingContext(sparkConf, Seconds.apply(15));

    // Set log level
    jsc.sparkContext().setLogLevel("INFO");

    JavaDStream<SparkPubsubMessage> stream = null;
    for (int i = 0; i < 5; i += 1) {
      JavaDStream<SparkPubsubMessage> pubSubReciever =
          PubsubUtils.createStream(
              jsc,
              "ikame-gem-ai-research",
              "test-1-sub",
              new SparkGCPCredentials.Builder().build(),
              StorageLevel.MEMORY_AND_DISK_SER());
      if (stream == null) {
        stream = pubSubReciever;
      } else {
        stream = stream.union(pubSubReciever);
      }
    }

    writeToBQ(stream, "ikame-gem-ai-research", "Adjust_realtime", "test_1", 1000);

    jsc.start();
    jsc.awaitTerminationOrTimeout(600000);

    jsc.stop();
  }

  public static void writeToBQ(
      JavaDStream<SparkPubsubMessage> pubSubStream,
      String outputProjectID,
      String pubSubBQOutputDataset,
      String PubSubBQOutputTable,
      Integer batchSize) {
    pubSubStream.foreachRDD(
        new VoidFunction<JavaRDD<SparkPubsubMessage>>() {
          @Override
          public void call(JavaRDD<SparkPubsubMessage> sparkPubsubMessageJavaRDD) throws Exception {
            sparkPubsubMessageJavaRDD.foreachPartition(
                new VoidFunction<Iterator<SparkPubsubMessage>>() {
                  @Override
                  public void call(Iterator<SparkPubsubMessage> sparkPubsubMessageIterator)
                      throws Exception {
                    BigQueryWriteClient client = BigQueryWriteClient.create();
                    TableName parentTable =
                        TableName.of(outputProjectID, pubSubBQOutputDataset, PubSubBQOutputTable);

                    BatchStreamBq.DataWriter writer = new BatchStreamBq.DataWriter();
                    // One time initialization.
                    writer.initialize(parentTable, client);

                    JSONArray jsonArr = new JSONArray();
                    long offset = 0;
                    while (sparkPubsubMessageIterator.hasNext()) {
                      SparkPubsubMessage message = sparkPubsubMessageIterator.next();
                      JSONObject record = new JSONObject(new String(message.getData()));
                      jsonArr.put(record);
                      if (jsonArr.length() == batchSize) {
                        writer.append(jsonArr, offset);
                        offset += jsonArr.length();
                      }
                    }
                    if (jsonArr.length() > 0) {
                      writer.append(jsonArr, offset);
                    }
                    writer.cleanup(client);
                    BatchCommitWriteStreamsRequest commitRequest =
                        BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(parentTable.toString())
                            .addWriteStreams(writer.getStreamName())
                            .build();
                    BatchCommitWriteStreamsResponse commitResponse =
                        client.batchCommitWriteStreams(commitRequest);
                    if (!commitResponse.hasCommitTime()) {
                      for (StorageError err : commitResponse.getStreamErrorsList()) {
                        System.out.println(err.getErrorMessage());
                      }
                      throw new RuntimeException("Error committing the streams");
                    }
                    client.shutdown();
                    //                    System.out.println("Appended and committed records
                    // successfully.");
                  }
                });
          }
        });
  }
}
