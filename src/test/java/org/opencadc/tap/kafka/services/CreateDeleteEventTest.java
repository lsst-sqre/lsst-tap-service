package org.opencadc.tap.kafka.services;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencadc.tap.kafka.KafkaConfig;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;

public class CreateDeleteEventTest {

    private static final String TEST_DELETE_TOPIC = "test-delete-topic";
    private static final String TEST_EXECUTION_ID = "test-execution-id";
    private static final String TEST_JOB_ID = "test-job-id";
    private static final String TEST_OWNER_ID = "test-owner-id";
    private static final String TEST_QUERY_TOPIC = "test-query-topic";
    private static final String TEST_STATUS_TOPIC = "test-status-topic";

    // Start a Kafka container
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.2"));

    private KafkaConfig kafkaConfig;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> consumer;

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
        
        if (!topicNames.contains(TEST_DELETE_TOPIC)) {
            NewTopic topic = new NewTopic(TEST_DELETE_TOPIC, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(topic)).all().get(30, TimeUnit.SECONDS);
        }
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TEST_DELETE_TOPIC));
        
        kafkaConfig = new KafkaConfig(
            bootstrapServers,
            TEST_QUERY_TOPIC,
            TEST_STATUS_TOPIC,
            TEST_DELETE_TOPIC
        );
    }

    @After
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
        
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Test
    public void testSubmitDeletion() throws Exception {
        CreateDeleteEvent service = new CreateDeleteEvent(kafkaConfig, 30);
        
        String result = service.submitDeletion(TEST_EXECUTION_ID, TEST_OWNER_ID, TEST_JOB_ID);
        
        assertEquals(TEST_EXECUTION_ID, result);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        assertFalse("No messages received from Kafka", records.isEmpty());
        
        boolean messageFound = false;
        for (ConsumerRecord<String, String> record : records) {
            if (TEST_EXECUTION_ID.equals(record.key())) {
                String value = record.value();
                assertTrue("Message should contain executionID", value.contains(TEST_EXECUTION_ID));
                assertTrue("Message should contain ownerID", value.contains(TEST_OWNER_ID));
                messageFound = true;
                break;
            }
        }
        
        assertTrue("Expected message not found in Kafka", messageFound);
        service.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmitDeletionWithNullExecutionID() throws Exception {
        CreateDeleteEvent service = new CreateDeleteEvent(kafkaConfig);
        try {
            service.submitDeletion(null, TEST_OWNER_ID, TEST_JOB_ID);
        } finally {
            service.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmitDeletionWithEmptyExecutionID() throws Exception {
        CreateDeleteEvent service = new CreateDeleteEvent(kafkaConfig);
        try {
            service.submitDeletion("", TEST_OWNER_ID, TEST_JOB_ID);
        } finally {
            service.close();
        }
    }

    @Test
    public void testCloseMethod() throws Exception {
        CreateDeleteEvent service = new CreateDeleteEvent(kafkaConfig);
        
        service.submitDeletion(TEST_EXECUTION_ID, TEST_OWNER_ID, TEST_JOB_ID);
        service.close();
        
        try {
            service.submitDeletion(TEST_EXECUTION_ID + "-2", TEST_OWNER_ID, TEST_JOB_ID);
            fail("Expected IllegalStateException was not thrown");
        } catch (IllegalStateException e) {
        }
    }
}