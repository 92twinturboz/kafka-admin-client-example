package com.dwittekind.ccloud;

import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientExample {
    private AdminClient client = null;

    public void setup() {
        // Ideally, you would import these settings from a properties file or the like
        Properties props = new Properties();
        props.setProperty("ssl.endpoint.identification.algorithm", "https");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "<broker_endpoint>");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<API_KEY>\" password=\"<API_SECRET>\";");
        props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        client = AdminClient.create(props);
    }
    public void teardown() {
        client.close();
    }

    public void topicListing() throws InterruptedException, ExecutionException {
        ListTopicsResult ltr = client.listTopics();
        KafkaFuture<Set<String>> names = ltr.names();
        System.out.println(names.get());
    }

    public void createTestTopic() {
        int partitions = 8;
        short replicationFactor = 3;
        try {
            KafkaFuture<Void> future = client
                    .createTopics(Collections.singleton(new NewTopic("admin-client-topic", partitions, replicationFactor)),
                            new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void updateTestTopic() throws InterruptedException, ExecutionException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "admin-client-topic");
        // Prints the existing topic config
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> config = result.all().get();
        System.out.println("*** CONFIG BEFORE ***");
        System.out.println(config);
        // Changing the retention ms on the topic
        ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "60000");
        Map<ConfigResource, Collection<AlterConfigOp>> updateConfig = new HashMap<ConfigResource, Collection<AlterConfigOp>>();
        updateConfig.put(resource, Collections.singleton(new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET)));
        AlterConfigsResult alterConfigsResult = client.incrementalAlterConfigs(updateConfig);
        alterConfigsResult.all();
        // Prints the new topic config
        result = client.describeConfigs(Collections.singleton(resource));
        config = result.all().get();
        System.out.println("*** CONFIG AFTER ***");
        System.out.println(config);

    }

    public void deleteTestTopic() {
        KafkaFuture<Void> future = client.deleteTopics(Collections.singleton("admin-client-topic")).all();
        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

}
