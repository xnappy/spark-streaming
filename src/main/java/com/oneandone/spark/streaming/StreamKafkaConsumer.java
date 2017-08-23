package com.oneandone.spark.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oneandone.alarming.mail.MailSender;
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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class StreamKafkaConsumer {
    private static long durationThreshold = 5000;

    protected static Properties configProperties = new Properties();

    private static final String ALARMING_MAIL_SMTP_HOST_PROPERTY = "alarming.mail.smtp.host";
    private static final String ALARMING_MAIL_SMTP_PORT_PROPERTY = "alarming.mail.smtp.port";
    private static final String ALARMING_MAIL_FROM_PROPERTY = "alarming.mail.from";
    private static final String ALARMING_MAIL_TO_PROPERTY = "alarming.mail.to";

    private static final String DEFAULT_ALARMING_MAIL_SMTP_HOST = "mxintern.schlund.de";
    private static final String DEFAULT_ALARMING_MAIL_SMTP_PORT = "25";
    private static final String DEFAULT_ALARMING_MAIL_FROM = "devnull@1und1.de";
    private static final String DEFAULT_ALARMING_MAIL_TEMPLATE_PATH = "/bam_alarming_mail.template";

    public static void main(String[] args) {
        //System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop");
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
            SparkSession sparkSession = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
            JavaRDD<BamRecord> rowRDD = rdd.map(eventMapper -> {
                ObjectMapper mapper = new ObjectMapper();
                BamRecord record = mapper.readValue(eventMapper, BamRecord.class);
                return record;
            });

            // Spark DataFrame with all events --> TempView: events
            Dataset<Row> events = sparkSession.createDataFrame(rowRDD, BamRecord.class);
            events.cache();
            events.createOrReplaceTempView("events");
            System.out.println("============ ALL EVENTS ============");
            events.show(false);

            // Spark DataFrame with only start events --> TempView: startEvents
            Dataset<Row> startEvents = sparkSession.sql(
                    "SELECT version AS sVersion, timestamp AS sTimestamp, documentType AS sDocumentType, eventType AS sEventType, technicalOrigin AS sTechnicalOrigin, businessOrigin AS sBusinessOrigin, businessProcessId AS sBusinessProcessId, businessProcessName AS sBusinessProcessName, businessProcessVersion AS sBusinessProcessVersion, businessProcessActivityId AS sBusinessProcessActivityId, businessProcessActivityName AS sBusinessProcessActivityName, businessProcessInstanceId AS sBusinessProcessInstanceId, payload.spp_trace_wf_id AS sSpp_trace_wf_id " +
                            "FROM events WHERE eventType = 'ACTIVITY_START'"
            );
            startEvents.cache();
            startEvents.createOrReplaceTempView("startEvents");

            // Spark DataFrame with only end events --> TempView: endEvents
            Dataset<Row> endEvents = sparkSession.sql(
                    "SELECT version AS eVersion, timestamp AS eTimestamp, documentType AS eDocumentType, eventType AS eEventType, technicalOrigin AS eTechnicalOrigin, businessOrigin AS eBusinessOrigin, businessProcessId AS eBusinessProcessId, businessProcessName AS eBusinessProcessName, businessProcessVersion AS eBusinessProcessVersion, businessProcessActivityId AS eBusinessProcessActivityId, businessProcessActivityName AS eBusinessProcessActivityName, businessProcessInstanceId AS eBusinessProcessInstanceId, durationInMillis, payload.spp_trace_wf_id AS eSpp_trace_wf_id, payload.ipCheckResult AS ipCheckResult " +
                            "FROM events WHERE eventType = 'ACTIVITY_END'"
            );
            endEvents.cache();
            endEvents.createOrReplaceTempView("endEvents");

            // Join Start and End Events (FULL OUTER)
            Dataset<Row> correlatedEvents = startEvents.join(endEvents, startEvents.col("sBusinessProcessName").equalTo(endEvents.col("eBusinessProcessName"))
                            .and(startEvents.col("sBusinessProcessActivityId").equalTo(endEvents.col("eBusinessProcessActivityId")))
                            .and(startEvents.col("sSpp_trace_wf_id").equalTo(endEvents.col("eSpp_trace_wf_id"))),"full_outer");
            correlatedEvents.cache();
            correlatedEvents.createOrReplaceTempView("correlatedEvents");

            // Start Events with No End
                Dataset<Row> startEventsWithoutEnd = sparkSession.sql(
                    "SELECT sVersion, sTimestamp, sDocumentType, sEventType, sTechnicalOrigin, sBusinessOrigin, sBusinessProcessId, sBusinessProcessName, sBusinessProcessVersion, sBusinessProcessActivityId, sBusinessProcessActivityName, sBusinessProcessInstanceId, sSpp_trace_wf_id " +
                            "FROM correlatedEvents WHERE eTimestamp IS NULL"
            );
            startEventsWithoutEnd.cache();
            System.out.println("============ START EVENTS WITH NO END EVENT ============");
            startEventsWithoutEnd.show();

            // End Events with No Start
            Dataset<Row> endEventsWithoutStart = sparkSession.sql(
                    "SELECT eVersion, eTimestamp, eDocumentType, eEventType, eTechnicalOrigin, eBusinessOrigin, eBusinessProcessId, eBusinessProcessName, eBusinessProcessVersion, eBusinessProcessActivityId, eBusinessProcessActivityName, eBusinessProcessInstanceId, durationInMillis, eSpp_trace_wf_id, ipCheckResult " +
                            "FROM correlatedEvents WHERE sTimestamp IS NULL"
            );
            endEventsWithoutStart.cache();
            System.out.println("============ END EVENTS WITH NO START EVENT ============");
            endEventsWithoutStart.show();

            // Finished Events - enrich data of END EVENT for elastic
            Dataset<Row> finishedEvents = sparkSession.sql(
                    "SELECT eVersion AS version, eTimestamp AS timestamp, eDocumentType AS documentType, eEventType AS eventType, eTechnicalOrigin AS technicalOrigin, eBusinessOrigin AS businessOrigin, eBusinessProcessId AS businessProcessId, eBusinessProcessName AS businessProcessName, eBusinessProcessVersion AS businessProcessVersion, eBusinessProcessActivityId AS businessProcessActivityId, eBusinessProcessActivityName AS businessProcessActivityName, eBusinessProcessInstanceId AS businessProcessInstanceId, durationInMillis, eSpp_trace_wf_id AS spp_trace_wf_id, ipCheckResult " +
                            "FROM correlatedEvents WHERE sTimestamp IS NOT NULL AND eTimestamp IS NOT NULL"
            );
            finishedEvents.cache();
            finishedEvents.createOrReplaceTempView("finishedEvents");
            System.out.println("============ FINISHED EVENTS ============");
            finishedEvents.show();

            // Check what to do next
            if (finishedEvents.groupBy("businessProcessName", "spp_trace_wf_id").count().count() > 0){
                writeFinishedEvents(sparkSession, finishedEvents);
            }
            if (startEventsWithoutEnd.groupBy("sBusinessProcessName", "sSpp_trace_wf_id").count().count() > 0){
                eventsWithoutEnd(startEventsWithoutEnd);
            }
            if (endEventsWithoutStart.groupBy("eBusinessProcessName", "eSpp_trace_wf_id").count().count() > 0){
                eventsWithoutStart(sparkSession, endEventsWithoutStart);
            }
        });

        //Start the computation
        streamingContext.start();
        try{
            streamingContext.awaitTermination();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    private static void eventsWithoutEnd(Dataset<Row> startEventsWithoutEnd){
        JavaEsSparkSQL.saveToEs(startEventsWithoutEnd,"unfinished/bam");
    }

    private static void eventsWithoutStart(SparkSession sparkSession, Dataset<Row> endEventsWithoutStart){
        Dataset<Row> startEventsWithoutEnd = JavaEsSparkSQL.esDF(sparkSession,"unfinished/bam");
        startEventsWithoutEnd.cache();

        Dataset<Row> joinedEvents = endEventsWithoutStart.join(startEventsWithoutEnd,
                startEventsWithoutEnd.col("sBusinessProcessName").equalTo(endEventsWithoutStart.col("eBusinessProcessName"))
                        .and(startEventsWithoutEnd.col("sBusinessProcessActivityId").equalTo(endEventsWithoutStart.col("eBusinessProcessActivityId")))
                        .and(startEventsWithoutEnd.col("sSpp_trace_wf_id").equalTo(endEventsWithoutStart.col("eSpp_trace_wf_id"))));
        joinedEvents.cache();
        joinedEvents.createOrReplaceTempView("joinedEvents");

        if (joinedEvents.groupBy("eBusinessProcessName", "eSpp_trace_wf_id").count().count() > 0){
            Dataset<Row> finishedEvents = sparkSession.sql(
                    "SELECT eVersion AS version, eTimestamp AS timestamp, eDocumentType AS documentType, eEventType AS eventType, eTechnicalOrigin AS technicalOrigin, eBusinessOrigin AS businessOrigin, eBusinessProcessId AS businessProcessId, eBusinessProcessName AS businessProcessName, eBusinessProcessVersion AS businessProcessVersion, eBusinessProcessActivityId AS businessProcessActivityId, eBusinessProcessActivityName AS businessProcessActivityName, eBusinessProcessInstanceId AS businessProcessInstanceId, durationInMillis, eSpp_trace_wf_id AS spp_trace_wf_id, ipCheckResult " +
                            "FROM joinedEvents WHERE sTimestamp IS NOT NULL AND eTimestamp IS NOT NULL"
            );
            finishedEvents.cache();

            // Upsert results into Elasticsearch
            Map<String, String> upsertConf = new HashMap<>();
            upsertConf.put("es.write.operation","upsert");
            upsertConf.put("es.mapping.id","id");
            upsertConf.put("es.mapping.timestamp", "@timestamp");
            upsertConf.put("es.mapping.version","@version");
            JavaEsSparkSQL.saveToEs(finishedEvents,"unfinished/bam", upsertConf);

            writeFinishedEvents(sparkSession,finishedEvents);
        }
    }

    private static void writeFinishedEvents(SparkSession sparkSession, Dataset<Row> finishedEvents){
        // UserDefinedFunctions
        sparkSession.udf().register("checkDurationString", (Long d) -> d > durationThreshold ? "NOK" : "OK", DataTypes.StringType);
        sparkSession.udf().register("checkDurationInteger", (Long d) -> d > durationThreshold ? 1 : 0, DataTypes.IntegerType);

        // Checking a KPI (phase running time) against configured threshold
        // AND creating final Events to write into elasticsearch with expanded payload
        Dataset<Row> checkedEvents = finishedEvents
                .withColumn("payload", map(lit("spp_trace_wf_id"), finishedEvents.col("spp_trace_wf_id"),
                        lit("ipCheckResult"), finishedEvents.col("ipCheckResult"),
                        lit("kpiThresholdValue"), lit(durationThreshold),
                        lit("kpiStatusText"), callUDF("checkDurationString", finishedEvents.col("durationInMillis")),
                        lit("kpiStatusValue"),callUDF("checkDurationInteger",finishedEvents.col("durationInMillis"))))
                .withColumn("documentType",lit("correlated_event"))
                .withColumn("id", concat_ws("_", finishedEvents.col("businessProcessName"), finishedEvents.col("businessProcessActivityName"), finishedEvents.col("spp_trace_wf_id")))
                .withColumn("timestampFormatted", to_utc_timestamp(finishedEvents.col("timestamp"),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
                .withColumn("kpiStatusText", callUDF("checkDurationString", finishedEvents.col("durationInMillis")));
        checkedEvents.cache();

        Dataset<Row> nokPhases = checkedEvents.filter(checkedEvents.col("kpiStatusText").equalTo(lit("NOK")));
        nokPhases.cache();
        if (nokPhases.count() > 0){
            //System.out.println("============ NOK / ALERT EVENTS ============");
            //nokPhases.show(10,false);

            List<Row> nokProcessInstances = nokPhases.select(nokPhases.col("id")).collectAsList();
            StringBuffer stringBuffer = new StringBuffer();

            for (Row row : nokProcessInstances){
                stringBuffer.append(row.get(0));
                stringBuffer.append(" ");
            }

            sendEmailNotification(finishedEvents.col("businessProcessName").toString(),finishedEvents.col("businessProcessActivityName").toString(),stringBuffer.toString(), durationThreshold);
        }

        Dataset<Row> writableEvents = checkedEvents.select(col("version").alias("@version"),col("timestampFormatted").alias("@timestamp"),
                col("documentType"),col("eventType"),col("technicalOrigin"),col("businessOrigin"),col("businessProcessId"),
                col("businessProcessName"),col("businessProcessVersion"),col("businessProcessActivityId"),
                col("businessProcessActivityName"),col("businessProcessInstanceId"),col("durationInMillis"),col("payload"),col("id"));
        writableEvents.cache();
        //System.out.println("============ CHECKED EVENTS ============");
        //writableEvents.show(false);

        // Write results into Elasticsearch
        DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        Date today = new Date();
        Map<String, String> esConf = new HashMap<>();
        esConf.put("es.mapping.id","id");
        esConf.put("es.mapping.timestamp", "@timestamp");
        esConf.put("es.mapping.version","@version");
        JavaEsSparkSQL.saveToEs(writableEvents,"bam-" + dateFormat.format(today) + "/bam", esConf);
    }

    private static void sendEmailNotification(String processName, String phaseName, String processInstanceIds, Long threshold){
        String pathToMailTemplate = DEFAULT_ALARMING_MAIL_TEMPLATE_PATH;

        String smtpHost = configProperties.getProperty(ALARMING_MAIL_SMTP_HOST_PROPERTY, DEFAULT_ALARMING_MAIL_SMTP_HOST);
        String smtpPort = configProperties.getProperty(ALARMING_MAIL_SMTP_PORT_PROPERTY, DEFAULT_ALARMING_MAIL_SMTP_PORT);
        String mailFrom = configProperties.getProperty(ALARMING_MAIL_FROM_PROPERTY, DEFAULT_ALARMING_MAIL_FROM);
        String mailAddressList = configProperties.getProperty(ALARMING_MAIL_TO_PROPERTY, "mincekara@united-internet.de");

        String mailSubject = "[BAM, alarm!] for Process Name ["+processName+"]";

        Properties mailContentProperties = new Properties();
        mailContentProperties.setProperty("subject", mailSubject);
        mailContentProperties.setProperty("processName", processName);
        mailContentProperties.setProperty("phaseName", phaseName);
        mailContentProperties.setProperty("processInstanceIds", processInstanceIds);
        mailContentProperties.setProperty("threshold", String.valueOf(threshold));

        MailSender.sendMail(mailAddressList, mailSubject, smtpHost, smtpPort, mailFrom, pathToMailTemplate, mailContentProperties);
        System.out.println("E-Mail successfully send");
    }
}
