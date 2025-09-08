package org.opencadc.tap.kafka.services;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencadc.tap.kafka.KafkaConfig;
import org.opencadc.tap.kafka.models.JobStatus;
import org.opencadc.tap.kafka.models.JobStatus.QueryInfo;
import org.opencadc.tap.kafka.models.JobStatus.ResultInfo;
import org.opencadc.tap.kafka.models.JobStatus.Metadata;
import org.opencadc.tap.kafka.services.ReadJobStatus.StatusListener;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class ReadJobStatusTest {
private static final Logger log = Logger.getLogger(ReadJobStatusTest.class);

private static final String TEST_JOB_ID = "job123";
private static final String TEST_QUERY_TOPIC = "test-query-topic";
private static final String TEST_STATUS_TOPIC = "test-status-topic-" + System.currentTimeMillis();
private static final String TEST_DELETE_TOPIC = "test-delete-topic";
private static final String TEST_GROUP_ID = "test-group-" + System.currentTimeMillis();

@ClassRule
public static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.3.2"));

private KafkaConfig kafkaConfig;
private AdminClient adminClient;
private KafkaProducer<String, String> producer;
private ReadJobStatus readJobStatus;

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

    if (!topicNames.contains(TEST_STATUS_TOPIC)) {
        NewTopic topic = new NewTopic(TEST_STATUS_TOPIC, 1, (short) 1);
        adminClient.createTopics(Collections.singleton(topic)).all().get(30, TimeUnit.SECONDS);
    }

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    producer = new KafkaProducer<>(producerProps);

    kafkaConfig = new KafkaConfig(
            bootstrapServers,
            TEST_QUERY_TOPIC,
            TEST_STATUS_TOPIC,
            TEST_DELETE_TOPIC,
            true);

    readJobStatus = new ReadJobStatus(kafkaConfig, TEST_GROUP_ID);
}

@After
public void tearDown() {
    if (producer != null) {
        producer.close();
    }

    if (adminClient != null) {
        adminClient.close();
    }

    if (readJobStatus != null) {
        readJobStatus.close();
    }
}

@Test
public void testReceiveStatusUpdate() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<JobStatus> receivedStatus = new AtomicReference<>();

    readJobStatus.addStatusListener(new StatusListener() {
        @Override
        public void onStatusUpdate(JobStatus status) {
            receivedStatus.set(status);
            latch.countDown();
        }
    });

    readJobStatus.subscribeToJob(TEST_JOB_ID);
    readJobStatus.start();
    Thread.sleep(1000);

    JobStatus statusUpdate = JobStatus.newBuilder()
        .setJobID(TEST_JOB_ID)
        .setStatus(JobStatus.ExecutionStatus.QUEUED)
        .setTimestampNow()
        .build();

    QueryInfo queryInfo = new QueryInfo();
    queryInfo.setStartTime(System.currentTimeMillis());
    queryInfo.setTotalChunks(10);
    queryInfo.setCompletedChunks(0);
    statusUpdate.setQueryInfo(queryInfo);

    Metadata metadata = new Metadata();
    metadata.setQuery("SELECT * FROM test_table");
    metadata.setDatabase("test_db");
    metadata.addUserTable("test_table");
    statusUpdate.setMetadata(metadata);

    producer.send(new ProducerRecord<>(TEST_STATUS_TOPIC, TEST_JOB_ID, statusUpdate.toJsonString())).get();

    boolean received = latch.await(10, TimeUnit.SECONDS);
    assertTrue("Status update was not received within timeout", received);

    JobStatus status = receivedStatus.get();
    assertNotNull("Received status should not be null", status);
    assertEquals("Job ID should match", TEST_JOB_ID, status.getJobID());
    assertEquals("Status should be QUEUED", JobStatus.ExecutionStatus.QUEUED, status.getStatus());
    
    assertNotNull("QueryInfo should not be null", status.getQueryInfo());
    assertEquals("Total chunks should match", Integer.valueOf(10), status.getQueryInfo().getTotalChunks());
    assertEquals("Completed chunks should match", Integer.valueOf(0), status.getQueryInfo().getCompletedChunks());
    
    assertNotNull("Metadata should not be null", status.getMetadata());
    assertEquals("Query should match", "SELECT * FROM test_table", status.getMetadata().getQuery());
    assertEquals("Database should match", "test_db", status.getMetadata().getDatabase());
    assertNotNull("User tables should not be null", status.getMetadata().getUserTables());
    assertEquals("Should have one user table", 1, status.getMetadata().getUserTables().size());
    assertEquals("User table should match", "test_table", status.getMetadata().getUserTables().get(0));
}

@Test
public void testTerminalStatusUnsubscribes() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<JobStatus> receivedStatus = new AtomicReference<>();

    StatusListener listener = new StatusListener() {
        @Override
        public void onStatusUpdate(JobStatus status) {
            if (TEST_JOB_ID.equals(status.getJobID())) {
                receivedStatus.set(status);
                latch.countDown();
            }
        }
    };
    readJobStatus.addStatusListener(listener);

    readJobStatus.subscribeToJob(TEST_JOB_ID);

    readJobStatus.start();

    Thread.sleep(2000);

    JobStatus statusUpdate = JobStatus.newBuilder()
        .setJobID(TEST_JOB_ID)
        .setStatus(JobStatus.ExecutionStatus.COMPLETED)
        .setTimestampNow()
        .build();
        
    ResultInfo resultInfo = new ResultInfo();
    resultInfo.setTotalRows(100);
    resultInfo.setResultLocation("https://example.com/results/job123");
    ResultInfo.Format format = new ResultInfo.Format();
    format.setType("VOTable");
    format.setSerialization("BINARY2");
    resultInfo.setFormat(format);
    statusUpdate.setResultInfo(resultInfo);

    producer.send(new ProducerRecord<>(TEST_STATUS_TOPIC, TEST_JOB_ID, statusUpdate.toJsonString())).get();

    boolean received = latch.await(10, TimeUnit.SECONDS);
    assertTrue("Status update was not received within timeout", received);
    
    JobStatus status = receivedStatus.get();
    assertNotNull("Received status should not be null", status);
    assertTrue("Status should be terminal", status.getStatus().isTerminal());

    java.lang.reflect.Field subscribedJobIdsField = ReadJobStatus.class.getDeclaredField("subscribedJobIds");
    subscribedJobIdsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> subscribedJobIds = (Set<String>) subscribedJobIdsField.get(readJobStatus);
    

}

@Test
public void testMultipleStatusUpdatesForSameJob() throws Exception {
    CountDownLatch latch = new CountDownLatch(3);
    List<JobStatus> receivedUpdates = Collections.synchronizedList(new ArrayList<>());

    readJobStatus.addStatusListener(new StatusListener() {
        @Override
        public void onStatusUpdate(JobStatus status) {
            if (TEST_JOB_ID.equals(status.getJobID())) {
                receivedUpdates.add(status);
                latch.countDown();
            }
        }
    });

    readJobStatus.subscribeToJob(TEST_JOB_ID);
    readJobStatus.start();

    Thread.sleep(1000);

    JobStatus queued = JobStatus.newBuilder()
        .setJobID(TEST_JOB_ID)
        .setStatus(JobStatus.ExecutionStatus.QUEUED)
        .setTimestampNow()
        .build();
        
    Metadata metadata = new Metadata();
    metadata.setQuery("SELECT * FROM test_table");
    metadata.setDatabase("test_db");
    queued.setMetadata(metadata);
    
    JobStatus executing = JobStatus.newBuilder()
        .setJobID(TEST_JOB_ID)
        .setStatus(JobStatus.ExecutionStatus.EXECUTING)
        .setTimestampNow()
        .build();
        
    QueryInfo queryInfo = new QueryInfo();
    queryInfo.setStartTime(System.currentTimeMillis());
    queryInfo.setTotalChunks(10);
    queryInfo.setCompletedChunks(5);
    queryInfo.setEstimatedTimeRemaining(30);
    executing.setQueryInfo(queryInfo);
    
    JobStatus completed = JobStatus.newBuilder()
        .setJobID(TEST_JOB_ID)
        .setStatus(JobStatus.ExecutionStatus.COMPLETED)
        .setTimestampNow()
        .build();
        
    QueryInfo finalQueryInfo = new QueryInfo();
    finalQueryInfo.setStartTime(System.currentTimeMillis() - 60000);
    finalQueryInfo.setEndTime(System.currentTimeMillis());
    finalQueryInfo.setDuration(60);
    finalQueryInfo.setTotalChunks(10);
    finalQueryInfo.setCompletedChunks(10);
    
    ResultInfo resultInfo = new ResultInfo();
    resultInfo.setTotalRows(1000);
    resultInfo.setResultLocation("https://example.com/results/job123");
    ResultInfo.Format format = new ResultInfo.Format();
    format.setType("VOTable");
    format.setSerialization("BINARY2");
    resultInfo.setFormat(format);
    
    completed.setQueryInfo(finalQueryInfo);
    completed.setResultInfo(resultInfo);

    producer.send(new ProducerRecord<>(TEST_STATUS_TOPIC, TEST_JOB_ID, queued.toJsonString())).get();
    Thread.sleep(100);
    
    producer.send(new ProducerRecord<>(TEST_STATUS_TOPIC, TEST_JOB_ID, executing.toJsonString())).get();
    Thread.sleep(100);
    
    producer.send(new ProducerRecord<>(TEST_STATUS_TOPIC, TEST_JOB_ID, completed.toJsonString())).get();

    boolean allReceived = latch.await(10, TimeUnit.SECONDS);
    assertTrue("Not all status updates were received", allReceived);
    
    Thread.sleep(1000);
    
    List<JobStatus.ExecutionStatus> statuses = new ArrayList<>();
    for (JobStatus status : receivedUpdates) {
        if (!statuses.contains(status.getStatus())) {
            statuses.add(status.getStatus());
        }
    }
    
    assertEquals("Should have received 3 different status types", 3, statuses.size());
    assertTrue("Should include QUEUED status", statuses.contains(JobStatus.ExecutionStatus.QUEUED));
    assertTrue("Should include EXECUTING status", statuses.contains(JobStatus.ExecutionStatus.EXECUTING));
    assertTrue("Should include COMPLETED status", statuses.contains(JobStatus.ExecutionStatus.COMPLETED));
    
    JobStatus queuedStatus = null;
    JobStatus executingStatus = null;
    JobStatus completedStatus = null;
    
    for (JobStatus status : receivedUpdates) {
        if (status.getStatus() == JobStatus.ExecutionStatus.QUEUED) {
            queuedStatus = status;
        } else if (status.getStatus() == JobStatus.ExecutionStatus.EXECUTING) {
            executingStatus = status;
        } else if (status.getStatus() == JobStatus.ExecutionStatus.COMPLETED) {
            completedStatus = status;
        }
    }
    
    assertNotNull("Should have received QUEUED status", queuedStatus);
    assertNotNull("QUEUED status should have metadata", queuedStatus.getMetadata());
    assertEquals("Query should match", "SELECT * FROM test_table", queuedStatus.getMetadata().getQuery());
    
    assertNotNull("Should have received EXECUTING status", executingStatus);
    assertNotNull("EXECUTING status should have query info", executingStatus.getQueryInfo());
    assertEquals("Completed chunks should be 5", Integer.valueOf(5), executingStatus.getQueryInfo().getCompletedChunks());
    
    assertNotNull("Should have received COMPLETED status", completedStatus);
    assertTrue("COMPLETED status should be terminal", completedStatus.getStatus().isTerminal());
    assertNotNull("COMPLETED status should have result info", completedStatus.getResultInfo());
    assertEquals("Total rows should be 1000", Integer.valueOf(1000), completedStatus.getResultInfo().getTotalRows());
}
}