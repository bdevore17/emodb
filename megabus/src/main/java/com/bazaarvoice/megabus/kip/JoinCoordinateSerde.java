package com.bazaarvoice.megabus.kip;

import java.util.Arrays;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Factory for creating JoinCoordinate serializers / deserializers.
 */
public class JoinCoordinateSerde<K, K0> implements Serde<JoinCoordinate<K, K0>> {
    final private Serializer<K> primaryKeySerializer;
    final private Deserializer<K> primaryKeyDeserializer;
    final private Serializer<K0> foreignKeySerializer;
    final private Deserializer<K0> foreignKeyDeserializer;
    final private Serializer<JoinCoordinate<K, K0>> serializer;
    final private Deserializer<JoinCoordinate<K, K0>> deserializer;

    public JoinCoordinateSerde(final Serde<K> primaryKeySerde, final Serde<K0> foreignKeySerde) {
        this.primaryKeySerializer = primaryKeySerde.serializer();
        this.primaryKeyDeserializer = primaryKeySerde.deserializer();
        this.foreignKeyDeserializer = foreignKeySerde.deserializer();
        this.foreignKeySerializer = foreignKeySerde.serializer();
        this.serializer = new JoinCoordinateSerializer<>(primaryKeySerializer, foreignKeySerializer);
        this.deserializer = new JoinCoordinateDeserializer<>(primaryKeyDeserializer, foreignKeyDeserializer);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        primaryKeySerializer.configure(configs, isKey);
        foreignKeySerializer.configure(configs, isKey);
        primaryKeyDeserializer.configure(configs, isKey);
        foreignKeyDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        primaryKeyDeserializer.close();
        foreignKeyDeserializer.close();
        primaryKeySerializer.close();
        foreignKeySerializer.close();
    }

    @Override
    public Serializer<JoinCoordinate<K, K0>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<JoinCoordinate<K, K0>> deserializer() {
        return deserializer;
    }

    public Serializer<K0> getForeignKeySerializer() {
        return this.foreignKeySerializer;
    }

    public Serializer<K> getPrimaryKeySerializer() {
        return this.primaryKeySerializer;
    }

    class JoinCoordinateSerializer<KP, KF> implements Serializer<JoinCoordinate<KP, KF>> {

        private final Serializer<KF> foreignKeySerializer;
        private final Serializer<KP> primaryKeySerializer;

        public JoinCoordinateSerializer(final Serializer<KP> primaryKeySerializer, final Serializer<KF> foreignKeySerializer) {
            this.foreignKeySerializer = foreignKeySerializer;
            this.primaryKeySerializer = primaryKeySerializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Don't need to configure, they are already configured. This is just a wrapper.
        }

        @Override
        public byte[] serialize(final String topic, final JoinCoordinate<KP, KF> data) {
            //{Integer.BYTES foreignKeyLength}{foreignKeySerialized}{primaryKeySerialized}
            final byte[] foreignKeySerializedData = foreignKeySerializer.serialize(topic, data.getForeignKey());
            //Integer.BYTES bytes
            final byte[] foreignKeyByteSize = numToBytes(foreignKeySerializedData.length);

            if (data.getKey() != null) {
                //? bytes
                final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(topic, data.getKey());

                final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length + primaryKeySerializedData.length);
                buf.put(foreignKeyByteSize);
                buf.put(foreignKeySerializedData);
                buf.put(primaryKeySerializedData);
                return buf.array();
            } else {
                final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + foreignKeySerializedData.length);
                buf.put(foreignKeyByteSize);
                buf.put(foreignKeySerializedData);
                return buf.array();
            }
        }

        private byte[] numToBytes(final int num) {
            final ByteBuffer wrapped = ByteBuffer.allocate(4);
            wrapped.putInt(num);
            return wrapped.array();
        }

        @Override
        public void close() {
            foreignKeySerializer.close();
            primaryKeySerializer.close();
        }
    }

    class JoinCoordinateDeserializer<KP, KF> implements Deserializer<JoinCoordinate<KP, KF>> {

        private final Deserializer<KF> foreignKeyDeserializer;
        private final Deserializer<KP> primaryKeyDeserializer;


        public JoinCoordinateDeserializer(final Deserializer<KP> primaryKeyDeserializer, final Deserializer<KF> foreignKeyDeserializer) {
            this.foreignKeyDeserializer = foreignKeyDeserializer;
            this.primaryKeyDeserializer = primaryKeyDeserializer;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            //Don't need to configure them, as they are already configured. This is only a wrapper.
        }

        @Override
        public JoinCoordinate<KP, KF> deserialize(final String topic, final byte[] data) {
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final int foreignKeyLength = buf.getInt();
            final byte[] foreignKeyRaw = new byte[foreignKeyLength];
            buf.get(foreignKeyRaw, 0, foreignKeyLength);
            final KF foreignKey = foreignKeyDeserializer.deserialize(topic, foreignKeyRaw);

            if (data.length == Integer.BYTES + foreignKeyLength) {
                return new JoinCoordinate<>(null, foreignKey);
            } else {
                final byte[] primaryKeyRaw = new byte[data.length - foreignKeyLength - Integer.BYTES];
                buf.get(primaryKeyRaw, 0, primaryKeyRaw.length);
                final KP primaryKey = primaryKeyDeserializer.deserialize(topic, primaryKeyRaw);
                return new JoinCoordinate<>(primaryKey, foreignKey);
            }
        }

        @Override
        public void close() {
            foreignKeyDeserializer.close();
            primaryKeyDeserializer.close();
        }
    }

    public JoinCoordinate<K, K0> foreignKeyPlusOne(JoinCoordinate<K, K0> joinCoordinate) {
        final byte[] incrementedForeignKeyBytes = increment(foreignKeySerializer.serialize(null, joinCoordinate.getForeignKey()));
        final K0 incrementedForeignKey = foreignKeyDeserializer.deserialize(null, incrementedForeignKeyBytes);
        return new JoinCoordinate<>(null, incrementedForeignKey);
    }

    /**
     * Increment the underlying byte array by adding 1.
     *
     * @param input - The byte array to increment
     * @return A new copy of the incremented byte array.
     */
    private static byte[] increment(byte[] input) {
        byte[] inputArr = input;
        byte[] ret = new byte[inputArr.length + 1];
        int carry = 1;
        for(int i = inputArr.length-1; i >= 0; i--) {
            if (inputArr[i] == (byte)0xFF && carry == 1) {
                ret[i] = (byte)0x00;
            } else {
                ret[i] = (byte)(inputArr[i] + carry);
                carry = 0;
            }
        }
        if (carry == 0)
            return Arrays.copyOf(ret, inputArr.length);
        ret[0] = 1;
        return ret;
    }


}