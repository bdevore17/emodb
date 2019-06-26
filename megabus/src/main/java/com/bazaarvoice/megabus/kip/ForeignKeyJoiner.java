package com.bazaarvoice.megabus.kip;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ForeignKeyJoiner {

    public static <K, V, K0, V0, VR> KStream<K, VR> leftJoinOnForeignKey(StreamsBuilder streamsBuilder,
                                                                         KTable<K, V> leftTable,
                                                                         KTable<K0, V0> rightTable,
                                                                         ValueMapper<V, K0> foreignKeyExtractor,
                                                                         ValueJoiner<V, V0, VR> joiner,
                                                                         Serde<K> leftKeySerde,
                                                                         Serde<V> leftValueSerde,
                                                                         Serde<K0> rightKeySerde,
                                                                         Serde<V0> rightValueSerde) {

        JoinCoordinateSerde<K, K0> joinCoordinateSerde = new JoinCoordinateSerde<>(leftKeySerde, rightKeySerde);

        StoreBuilder<KeyValueStore<K, K0>> previousForeignKeyStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore("previousForeignKeyState"),
                        leftKeySerde,
                        rightKeySerde);

        StoreBuilder<KeyValueStore<JoinCoordinate<K, K0>, byte[]>> joinStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore("joinStore"),
                        joinCoordinateSerde,
                        Serdes.ByteArray());

        streamsBuilder.addStateStore(previousForeignKeyStore);
        streamsBuilder.addStateStore(joinStore);

        KStream<JoinCoordinate<K, K0>, MessageWithSourceAndHeaders> leftStream = leftTable.toStream()
                .flatTransform(() -> new ForeignKeyMessageRouter<>(foreignKeyExtractor), "previousForeignKeyState")
                .transformValues(() -> new HeaderAddingTransformer<>())
                .map((key, value) -> new KeyValue<>(new JoinCoordinate<>(key, value.getForeignKey()), value.getValue()))
                .through("intermediate-join", Produced.with(joinCoordinateSerde, leftValueSerde, new ForeignKeyPartitioner<>(rightKeySerde.serializer())))
                .transformValues(() -> new MessageSourceTopicLineageTransformer<>(SourceTopic.LEFT, leftValueSerde.serializer()));

        KStream<JoinCoordinate<K, K0>, MessageWithSourceAndHeaders> rightStream = rightTable.toStream()
                .transformValues(() -> new MessageSourceTopicLineageTransformer<>(SourceTopic.RIGHT, rightValueSerde.serializer()))
                .map((key, value) -> new KeyValue<>(new JoinCoordinate<>(null, key), value));

        return leftStream.merge(rightStream)
                .flatTransform(() -> new JoinTransformer<>(joiner, joinCoordinateSerde, leftValueSerde, rightValueSerde));

    }

    private static class JoinTransformer<K, K0, V, V0, VR> implements Transformer<JoinCoordinate<K, K0>, MessageWithSourceAndHeaders, Iterable<KeyValue<K, VR>>> {

        private ProcessorContext context;
        private KeyValueStore<JoinCoordinate<K, K0>, byte[]> state;

        private final ValueJoiner<V, V0, VR> valueJoiner;

        private final JoinCoordinateSerde<K, K0> joinCoordinateSerde;

        private final Serde<V> leftValueSerde;
        private final Serde<V0> rightValueSerde;

        public JoinTransformer(ValueJoiner<V, V0, VR> valueJoiner, JoinCoordinateSerde<K, K0> joinCoordinateSerde,
                               Serde<V> leftValueSerde, Serde<V0> rightValueSerde) {
            this.valueJoiner = valueJoiner;
            this.joinCoordinateSerde = joinCoordinateSerde;
            this.leftValueSerde = leftValueSerde;
            this.rightValueSerde = rightValueSerde;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.state = (KeyValueStore<JoinCoordinate<K, K0>, byte[]>) context.getStateStore("previousForeignKeyState");
        }

        @Override
        public Iterable<KeyValue<K, VR>> transform(JoinCoordinate<K, K0> key, MessageWithSourceAndHeaders message) {
            checkNotNull(message.getSourceTopic());
            Iterator<KeyValue<K, V>> left;
            V0 right;
            Runnable closeLeft;

            if (message.getSourceTopic() == SourceTopic.LEFT) {
                if (message.isRemoveOnly()) {
                    checkArgument(message.getValue() == null);
                    state.delete(key);
                    return Collections.emptySet();
                } else if (message.getValue() == null) {
                    state.delete(key);
                    return Collections.singleton(new KeyValue<>(key.getKey(), null));
                } else {
                    state.put(key, message.getValue());
                    right = rightValueSerde.deserializer().deserialize(null, state.get(new JoinCoordinate<>(null, key.getForeignKey())));
                    left = Collections.singleton(new KeyValue<K, V>(key.getKey(), leftValueSerde.deserializer().deserialize(null, message.getValue()))).iterator();
                    closeLeft = () -> {};
                }
            } else {
                checkArgument(!message.isRemoveOnly());
                if (message.getValue() == null) {
                    state.delete(key);
                    right = null;
                } else {
                    state.put(key, message.getValue());
                    right = rightValueSerde.deserializer().deserialize(null, message.getValue());
                }

                KeyValueIterator<JoinCoordinate<K, K0>, byte[]> keyValueIterator =
                        state.range(key, joinCoordinateSerde.foreignKeyPlusOne(key));
                Iterator<KeyValue<JoinCoordinate<K, K0>, byte[]>> filteredIterator =
                        Iterators.filter(keyValueIterator, kv -> kv.key.getForeignKey().equals(key.getForeignKey()));
                left = Iterators.transform(filteredIterator, kv -> new KeyValue(kv.key.getKey(), kv.value));
                closeLeft = keyValueIterator::close;
            }

            List<KeyValue<K ,VR>> results = new ArrayList<>();
            while (left.hasNext()) {
                KeyValue<K, V> leftValue = left.next();
                results.add(new KeyValue<>(leftValue.key, valueJoiner.apply(leftValue.value, right)));
            }
            closeLeft.run();
            return results;
        }

        @Override
        public void close() {

        }
    }

    private enum SourceTopic {
        LEFT,
        RIGHT
    }

    private static class MessageSourceTopicLineageTransformer<VA> implements ValueTransformer<VA, MessageWithSourceAndHeaders> {

        private final SourceTopic sourceTopic;
        private ProcessorContext context;
        private Serializer<VA> valueSerializer;

        public MessageSourceTopicLineageTransformer(SourceTopic sourceTopic, Serializer<VA> valueSerializer) {
            this.sourceTopic = sourceTopic;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public MessageWithSourceAndHeaders transform(VA value) {
            boolean removeOnly = context.headers().headers(RemoveHeader.INSTANCE.key()).iterator().hasNext();
            checkArgument(value != null || removeOnly);

            return new MessageWithSourceAndHeaders(sourceTopic, removeOnly, valueSerializer.serialize(null, value));
        }

        @Override
        public void close() {

        }
    }

    private static class MessageWithSourceAndHeaders {
        private SourceTopic sourceTopic;
        private boolean removeOnly;
        private byte[] value;

        public MessageWithSourceAndHeaders(SourceTopic sourceTopic, boolean removeOnly, byte[] value) {
            this.sourceTopic = sourceTopic;
            this.removeOnly = removeOnly;
            this.value = value;
        }

        public SourceTopic getSourceTopic() {
            return sourceTopic;
        }

        public boolean isRemoveOnly() {
            return removeOnly;
        }

        public byte[] getValue() {
            return value;
        }
    }

    private static class ForeignKeyPartitioner<K, K0, V> implements StreamPartitioner<JoinCoordinate<K, K0>, V> {

        private final Serializer<K0> foreignKeySerializer;

        public ForeignKeyPartitioner(Serializer<K0> foreignKeySerializer) {
            this.foreignKeySerializer = foreignKeySerializer;
        }

        @Override
        public Integer partition(String topic, JoinCoordinate<K, K0> key, V value, int numPartitions) {
            return Utils.toPositive(Utils.murmur2(foreignKeySerializer.serialize(topic, key.getForeignKey()))) % numPartitions;
        }
    }

    private static class HeaderAddingTransformer<K0, V> implements ValueTransformer<ForeignKeyRoutingAction<K0, V>, ForeignKeyRoutingAction<K0, V>> {

        private ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public ForeignKeyRoutingAction<K0, V> transform(ForeignKeyRoutingAction<K0, V> foreignKeyRoutingAction) {
            if (foreignKeyRoutingAction.getAction() == Action.REMOVE) {
                context.headers().add(RemoveHeader.INSTANCE);
            }

            return foreignKeyRoutingAction;
        }

        @Override
        public void close() { }
    }

    private static class ForeignKeyMessageRouter<K, V, K0> implements Transformer<K, V, Iterable<KeyValue<K, ForeignKeyRoutingAction<K0, V>>>> {
        private ProcessorContext context;
        private KeyValueStore<K, K0> state;
        private ValueMapper<V, K0> foreignKeyExtractor;

        public ForeignKeyMessageRouter(ValueMapper<V, K0> foreignKeyExtractor) {
            this.foreignKeyExtractor = foreignKeyExtractor;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.state = (KeyValueStore<K, K0>) context.getStateStore("previousForeignKeyState");
        }

        @Override
        public Iterable<KeyValue<K, ForeignKeyRoutingAction<K0, V>>> transform(K key, V value) {

            K0 previousForeignKey = state.get(key);
            K0 foreignKey = value != null ? foreignKeyExtractor.apply(value) : null;

            if (previousForeignKey == null && foreignKey == null) {
                return Collections.emptySet();
            }

            if (previousForeignKey == null && foreignKey != null) {
                state.put(key, foreignKey);
                return Collections.singleton(new KeyValue<>(key, new ForeignKeyRoutingAction<>(Action.ADD, foreignKey, value)));
            }

            if (previousForeignKey != null && foreignKey == null) {
                state.delete(key);
                return Collections.singleton(new KeyValue<>(key, new ForeignKeyRoutingAction<>(Action.ADD, previousForeignKey, null)));
            }

            if (previousForeignKey != null && foreignKey != null) {
                if (previousForeignKey.equals(foreignKey)) {
                    return Collections.singleton(new KeyValue<>(key, new ForeignKeyRoutingAction<>(Action.ADD, foreignKey, value)));
                } else {
                    state.put(key, foreignKey);
                    return ImmutableSet.of(
                            new KeyValue<>(key, new ForeignKeyRoutingAction<>(Action.REMOVE, previousForeignKey, null)),
                            new KeyValue<>(key, new ForeignKeyRoutingAction<>(Action.ADD, foreignKey, value))
                    );
                }
            }

            throw new RuntimeException("This is impossible");
        }

        @Override
        public void close() {

        }
    }

    private enum Action {
        ADD,
        REMOVE
    }

    private static class ForeignKeyRoutingAction<K0, V> {

        private Action action;
        private K0 foreignKey;
        private V value;

        public ForeignKeyRoutingAction(Action action, K0 foreignKey, V value) {
            checkArgument(value != null || action == Action.REMOVE);
            this.action = action;
            this.foreignKey = foreignKey;
            this.value = value;
        }

        public Action getAction() {
            return action;
        }

        public K0 getForeignKey() {
            return foreignKey;
        }

        public V getValue() {
            return value;
        }
    }

    private static final class RemoveHeader implements Header {

        private static final RemoveHeader INSTANCE = new RemoveHeader();

        @Override
        public String key() {
            return "remove";
        }

        @Override
        public byte[] value() {
            return new byte[0];
        }
    }
}
