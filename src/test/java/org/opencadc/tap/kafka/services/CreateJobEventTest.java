package org.opencadc.tap.kafka.services;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencadc.tap.kafka.KafkaConfig;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat.Format;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat.Envelope;
import org.opencadc.tap.kafka.models.JobRun.ResultFormat.ColumnType;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import ca.nrc.cadc.vosi.TAPRegExtParser;

public class CreateJobEventTest {
    private static final Logger log = Logger.getLogger(CreateJobEventTest.class);

    private static final String TEST_QUERY = "SELECT * FROM test_table";
    private static final String TEST_JOB_ID = "k51tn910ak8wuc2z";
    private static final String TEST_OWNER_ID = "bot-mobu-tap";
    private static final String TEST_RESULT_DESTINATION = "test-destination";
    private static final String TEST_RESULT_LOCATION = "https://tap-files.lsst.codes/result_k51tn910ak8wuc2z.xml";
    private static final String TEST_DATABASE = "dp02";
    private static final String TEST_BASE_URL = "https://data-dev.lsst.cloud";

    private static final String TEST_QUERY_TOPIC = "test-query-topic";
    private static final String TEST_STATUS_TOPIC = "test-status-topic";
    private static final String TEST_DELETE_TOPIC = "test-delete-topic";
    private static final String TEST_UPLOAD_NAME = "test-upload-name";
    private static final String TEST_UPLOAD_SOURCE = "https://tap-files.lsst.codes/upload_k51tn910ak8wuc2z.xml";
    private static final Integer TEST_MAXREC = 1000;

    // Start a Kafka container
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.2"));

    private KafkaConfig kafkaConfig;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> consumer;
    private CreateJobEvent createJobEvent;

    @Before
    public void setUp() throws Exception {
        if (!kafka.isRunning()) {
            kafka.start();
        }

        String bootstrapServers = kafka.getBootstrapServers();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminProps);

        ListTopicsResult topics = adminClient.listTopics();
        Set<String> topicNames = topics.names().get(30, TimeUnit.SECONDS);

        if (!topicNames.contains(TEST_QUERY_TOPIC)) {
            NewTopic topic = new NewTopic(TEST_QUERY_TOPIC, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(topic)).all().get(30, TimeUnit.SECONDS);
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TEST_QUERY_TOPIC));

        kafkaConfig = new KafkaConfig(
                bootstrapServers,
                TEST_QUERY_TOPIC,
                TEST_STATUS_TOPIC,
                TEST_DELETE_TOPIC);

        createJobEvent = new CreateJobEvent(kafkaConfig, 30);
    }

    @After
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
        }

        if (adminClient != null) {
            adminClient.close();
        }

        if (createJobEvent != null) {
            createJobEvent.close();
        }
    }

    @Test
    public void testSubmitQuery() throws Exception {
        Format format = new Format("VOTable", "BINARY2");
        Envelope envelope = new Envelope("", "", "");

        List<ColumnType> columnTypes = new ArrayList<>();
        ColumnType column = new ColumnType("col1", "char");
        column.setArraysize("*");
        columnTypes.add(column);

        ResultFormat resultFormat = new ResultFormat(format, envelope, columnTypes, TEST_BASE_URL);

        String result = createJobEvent.submitQuery(
                TEST_QUERY,
                TEST_JOB_ID,
                TEST_RESULT_DESTINATION,
                TEST_RESULT_LOCATION,
                resultFormat,
                TEST_OWNER_ID,
                TEST_DATABASE,
                TEST_MAXREC,
                TEST_UPLOAD_NAME,
                TEST_UPLOAD_SOURCE);

        assertEquals(TEST_JOB_ID, result);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

        assertFalse("No messages received from Kafka", records.isEmpty());

        boolean messageFound = false;
        for (ConsumerRecord<String, String> record : records) {
            if (TEST_JOB_ID.equals(record.key())) {
                String value = record.value();

                JSONObject jsonMessage = new JSONObject(value);

                assertTrue("JSON should contain jobID", jsonMessage.has("jobID"));
                assertEquals(TEST_JOB_ID, jsonMessage.getString("jobID"));
                assertEquals(TEST_RESULT_DESTINATION, jsonMessage.getString("resultDestination"));
                assertEquals(TEST_RESULT_LOCATION, jsonMessage.getString("resultLocation"));
                assertEquals(TEST_QUERY, jsonMessage.getString("query"));
                assertEquals(TEST_OWNER_ID, jsonMessage.getString("ownerID"));
                assertEquals(TEST_DATABASE, jsonMessage.getString("database"));

                JSONObject resultFormatJson = jsonMessage.getJSONObject("resultFormat");

                if (resultFormatJson.has("format")) {
                    JSONObject formatJson = resultFormatJson.getJSONObject("format");
                    assertEquals("VOTable", formatJson.getString("type"));
                    assertEquals("BINARY2", formatJson.getString("serialization"));
                }

                if (resultFormatJson.has("envelope")) {
                    JSONObject envelopeJson = resultFormatJson.getJSONObject("envelope");
                    assertEquals("", envelopeJson.getString("header"));
                    assertEquals("", envelopeJson.getString("footer"));
                }

                if (resultFormatJson.has("baseUrl")) {
                    assertEquals(TEST_BASE_URL, resultFormatJson.getString("baseUrl"));
                }

                if (resultFormatJson.has("columnTypes") && resultFormatJson.getJSONArray("columnTypes").length() > 0) {
                    JSONObject columnJson = resultFormatJson.getJSONArray("columnTypes").getJSONObject(0);
                    assertEquals("col1", columnJson.getString("name"));
                    assertEquals("char", columnJson.getString("datatype"));
                    assertEquals("*", columnJson.getString("arraysize"));
                }

                messageFound = true;
                break;
            }
        }

        assertTrue("Expected message not found in Kafka", messageFound);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmitQuery_NullJobId() throws Exception {
        Format format = new Format("VOTable", "BINARY2");
        Envelope envelope = new Envelope("", "", "");
        List<ColumnType> columnTypes = new ArrayList<>();
        ResultFormat resultFormat = new ResultFormat(format, envelope, columnTypes);

        createJobEvent.submitQuery(
                TEST_QUERY,
                null,
                TEST_RESULT_DESTINATION,
                resultFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmitQuery_EmptyResultDestination() throws Exception {
        Format format = new Format("VOTable", "BINARY2");
        Envelope envelope = new Envelope("", "", "");
        List<ColumnType> columnTypes = new ArrayList<>();
        ResultFormat resultFormat = new ResultFormat(format, envelope, columnTypes);

        createJobEvent.submitQuery(
                TEST_QUERY,
                TEST_JOB_ID,
                "",
                resultFormat);
    }

    @Test
    public void testCloseMethod() throws Exception {

        Format format = new Format("VOTable", "BINARY2");
        Envelope envelope = new Envelope("", "", "");
        List<ColumnType> columnTypes = new ArrayList<>();
        ColumnType column = new ColumnType("col1", "char");
        column.setArraysize("*");
        columnTypes.add(column);
        ResultFormat resultFormat = new ResultFormat(format, envelope, columnTypes, TEST_BASE_URL);

        String result = createJobEvent.submitQuery(
                TEST_QUERY,
                TEST_JOB_ID + "-close",
                TEST_RESULT_DESTINATION,
                TEST_RESULT_LOCATION,
                resultFormat,
                TEST_OWNER_ID,
                TEST_DATABASE,
                TEST_MAXREC,
                TEST_UPLOAD_NAME,
                TEST_UPLOAD_SOURCE);

        assertEquals(TEST_JOB_ID + "-close", result);

        createJobEvent.close();

        try {
            createJobEvent.submitQuery(
                    TEST_QUERY,
                    TEST_JOB_ID + "-close-2",
                    TEST_RESULT_DESTINATION,
                    TEST_RESULT_LOCATION,
                    resultFormat,
                    TEST_OWNER_ID,
                    TEST_DATABASE,
                    TEST_MAXREC,
                    TEST_UPLOAD_NAME,
                    TEST_UPLOAD_SOURCE);
            fail("Expected IllegalStateException was not thrown");
        } catch (IllegalStateException e) {
        }
    }
}