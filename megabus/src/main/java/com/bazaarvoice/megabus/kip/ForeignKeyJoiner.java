package com.bazaarvoice.megabus.kip;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import static com.google.common.base.Preconditions.checkArgument;

public class ForeignKeyJoiner {

    public static <K, V, K0, V0, VR> KStream<K, VR> leftJoinOnForeignKey(KTable<K, V> leftTable,
                                                                         KTable<K0, V0> rightTable,
                                                                         ValueMapper<V, K0> foreignKeyExtractor,
                                                                         ValueJoiner<V, V0, VR> joiner,
                                                                         Serde<K> leftKeySerde,
                                                                         Serde<V> leftValueSerde,
                                                                         Serde<K0> rightKeySerde,
                                                                         Serde<V0> rightValueSerde,
                                                                         Serde<VR> joinedValueSerde) {
        StoreBuilder<KeyValueStore<K, K0>> previousForeignKeyStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore("previousForeignKeyState"),
                        leftKeySerde,
                        rightKeySerde);

        StoreBuilder<KeyValueStore<K, K0>> joinStore = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore("joinStore"),
                        leftKeySerde,
                        rightKeySerde);

        KStream<JoinCoordinate<K, K0>, MessageWithSourceAndHeaders> leftStream = leftTable.toStream()
                .flatTransform(() -> new ForeignKeyMessageRouter<>(foreignKeyExtractor), "previousForeignKeyState")
                .transformValues(() -> new HeaderAddingTransformer<>())
                .map((key, value) -> new KeyValue<>(new JoinCoordinate<>(key, value.getForeignKey()), value.getValue()))
                .through("intermediate-join", Produced.with(new JoinCoordinateSerde<>(leftKeySerde, rightKeySerde), leftValueSerde, new ForeignKeyPartitioner<>(rightKeySerde.serializer())))
                .transformValues(() -> new MessageSourceTopicLineageTransformer<>(SourceTopic.LEFT));

        KStream<JoinCoordinate<K, K0>, MessageWithSourceAndHeaders> rightStream = rightTable.toStream()
                .transformValues(() -> new MessageSourceTopicLineageTransformer<>(SourceTopic.RIGHT))
                .map((key, value) -> new KeyValue<>(new JoinCoordinate<>(null, key), value));
        
        return null;
    }

    private enum SourceTopic {
        LEFT,
        RIGHT
    }

    private static class MessageSourceTopicLineageTransformer<VA> implements ValueTransformer<VA, MessageWithSourceAndHeaders> {

        private final SourceTopic sourceTopic;
        private ProcessorContext context;

        public MessageSourceTopicLineageTransformer(SourceTopic sourceTopic) {
            this.sourceTopic = sourceTopic;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public MessageWithSourceAndHeaders transform(VA value) {
            boolean removeOnly = context.headers().headers(RemoveHeader.INSTANCE.key()).iterator().hasNext();
            checkArgument(value != null || removeOnly);

            return new MessageWithSourceAndHeaders(sourceTopic, removeOnly, value);
        }

        @Override
        public void close() {

        }
    }

    private static class MessageWithSourceAndHeaders {
        private SourceTopic sourceTopic;
        private boolean removeOnly;
        private Object value;

        public MessageWithSourceAndHeaders(SourceTopic sourceTopic, boolean removeOnly, Object value) {
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

        public Object getValue() {
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
