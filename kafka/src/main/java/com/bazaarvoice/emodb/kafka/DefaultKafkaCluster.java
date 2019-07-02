package com.bazaarvoice.emodb.kafka;

import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultKafkaCluster implements KafkaCluster {

    private final AdminClient _adminClient;
    private final String _bootstrapServers;
    private final String _instanceIdentifier;
    private final Supplier<Producer<String, JsonNode>> _producerSupplier;

    @Inject
    public DefaultKafkaCluster(AdminClient adminClient, @BootstrapServers String bootstrapServers,
                               @SelfHostAndPort HostAndPort hostAndPort) {
        _adminClient = checkNotNull(adminClient);
        _bootstrapServers = checkNotNull(bootstrapServers);
        _instanceIdentifier = checkNotNull(hostAndPort).toString();
        _producerSupplier = Suppliers.memoize(this::createProducer);

        Futures.getUnchecked(_adminClient.describeCluster().nodes()).forEach(System.out::println);
    }

    @Override
    public void createTopicIfNotExists(Topic topic) {
        NewTopic newTopic = new NewTopic(topic.getName(), topic.getPartitions(), topic.getReplicationFactor());

        try {
            _adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof TopicExistsException) {
                checkTopicPropertiesMatching(topic);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void checkTopicPropertiesMatching(Topic topic) {
            TopicDescription topicDescription = Futures.getUnchecked(
                    _adminClient.describeTopics(Collections.singleton(topic.getName())).all()).get(topic.getName());

            checkArgument(topicDescription.partitions().size() == topic.getPartitions());
            topicDescription.partitions().forEach(topicPartitionInfo ->
                    checkArgument(topicPartitionInfo.replicas().size() == topic.getReplicationFactor()));
    }

    private Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // 5 msloc

        props.put(ProducerConfig.CLIENT_ID_CONFIG, _instanceIdentifier);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new KafkaProducer<>(props);
    }

    // TODO: tune this
    public Producer<String, JsonNode> producer() {
        return _producerSupplier.get();
    }

    @Override
    public String getBootstrapServers() {
        return _bootstrapServers;
    }
}