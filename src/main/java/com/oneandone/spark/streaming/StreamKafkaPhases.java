package com.oneandone.spark.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oneandone.spark.streaming.BamRecord;
import com.oneandone.spark.streaming.JavaSparkSessionSingleton;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;

public class StreamKafkaPhases {

    public static void main(String[] args) {
        String topics = "test";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("SparkDirectStreamingSyscalls").setMaster("local[*]").set("es.nodes","10.88.11.48:9200");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(60));
        streamingContext.sparkContext().setLogLevel("WARN");

        // Create Kafka connection and direct kafka stream
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.88.10.75:9092");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        // Get Bam JSON Events from the messages
        JavaDStream<String> bamJsonEvents = messages.map((Function<Tuple2<String, String>, String>) stringStringTuple2 -> stringStringTuple2._2);
        bamJsonEvents.print();

        // Convert RDDs of the words DStream to DataFrame and run SQL query
        bamJsonEvents.foreachRDD((rdd, time) -> {
            String businessProcessName = "";
            String startActivityId = "";
            String startType = "";
            String endActivityId = "";
            String endType = "";
            String businessProcessPhaseName = "";
            long durationThreshold = 100;

            SparkSession sparkSession = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
            JavaRDD<BamRecord> rowRDD = rdd.map(eventMapper -> {
                ObjectMapper mapper = new ObjectMapper();
                BamRecord record = mapper.readValue(eventMapper, BamRecord.class);
                return record;
            });

            Dataset<Row> events = sparkSession.createDataFrame(rowRDD, BamRecord.class);
            events.cache();
            events.createOrReplaceTempView("events");
            System.out.println("============ ALL EVENTS ============");
            events.show(false);

            // Spark DataFrame with start events
            Dataset<Row> startEvents = sparkSession.sql(
                    "SELECT * FROM events " +
                            "WHERE businessProcessName ='" + businessProcessName + "' AND businessProcessActivityId ='" + startActivityId + "' AND eventType ='" + startType + "'");

            // Spark DataFrame with end events
            Dataset<Row> endEvents = sparkSession.sql(
                    "SELECT * FROM events " +
                            "WHERE businessProcessName ='" + businessProcessName + "' AND businessProcessActivityId ='" + endActivityId + "' AND eventType ='" + endType + "'");

            // Event correlation (JOIN) resulting in new Spark DataFrame
            sparkSession.udf().register("calcDuration", (Date start, Date end) -> end.getTime() - start.getTime(), DataTypes.LongType);
            Dataset<Row> calculatedPhases = startEvents.join(endEvents, "businessProcessInstanceId")
                    .withColumn("businessProcessPhaseName", lit(businessProcessPhaseName))
                    .withColumn("id", concat_ws("_", lit(businessProcessPhaseName), startEvents.col("businessProcessInstanceId")))
                    .withColumn("durationInMillis", callUDF("calcDuration", startEvents.col("startDate"), endEvents.col("endDate")));

            // Checking a KPI (phase running time) against configured threshold
            sparkSession.udf().register("checkDuration", (Long d) -> d > durationThreshold ? "NOK" : "OK", DataTypes.StringType);
            sparkSession.udf().register("checkDuration2", (Long d) -> d > durationThreshold ? 1 : 0, DataTypes.IntegerType);
            Dataset<Row> checkedPhases = calculatedPhases.withColumn("kpiStatusText", callUDF("checkDuration", calculatedPhases.col("durationInMillis")))
                    .withColumn("kpiThresholdValue", lit(durationThreshold))
                    .withColumn("kpiStatusValue", callUDF("checkDuration2", calculatedPhases.col("durationInMillis")))
                    .withColumn("documentType", lit("phase_event"));
            checkedPhases.show();

            // Writer results back into Elasticsearch
            Map<String, String> mappingConf = new HashMap<String, String>();
            mappingConf.put("es.mapping.id", "id");
            JavaEsSparkSQL.saveToEs(checkedPhases, "bam-phases/bam", mappingConf);
        });
    }
}
