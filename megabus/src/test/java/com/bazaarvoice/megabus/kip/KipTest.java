package com.bazaarvoice.megabus.kip;

import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import java.util.Collections;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.testng.annotations.Test;

public class KipTest {
    @Test
    public void test() {

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Review> reviewTable = builder.table("review", Consumed.with(Serdes.String(), new JsonPOJOSerde<>(Review.class)));
        KTable<String, Product> productTable = builder.table("product", Consumed.with(Serdes.String(), new JsonPOJOSerde<>(Product.class)));

        ForeignKeyJoiner.leftJoinOnForeignKey(builder, reviewTable, productTable, Review::getProductId,
                (review, product) -> new ReviewWithProductName(review.getRating(), product != null ? product.getName() : null),
                Serdes.String(), new JsonPOJOSerde<>(Review.class),
                Serdes.String(), new JsonPOJOSerde<>(Product.class))
        .to("joinStream", Produced.with(Serdes.String(), new JsonPOJOSerde<>(ReviewWithProductName.class)));

        Topology topology = builder.build();

// setup test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
        Properties properties = new Properties();

        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

        ConsumerRecordFactory<String, Product> productFactory = new ConsumerRecordFactory<>("product", new StringSerializer(), new JsonPOJOSerde<>(Product.class).serializer());
        ConsumerRecordFactory<String, Review> reviewFactory = new ConsumerRecordFactory<>("review", new StringSerializer(), new JsonPOJOSerde<>(Review.class).serializer());

        testDriver.pipeInput(productFactory.create("product", "product1", new Product("a name")));
        testDriver.pipeInput(reviewFactory.create("review", "review1", new Review(1, "product1")));
        testDriver.pipeInput(productFactory.create("product", "product1", new Product("another name")));
        testDriver.pipeInput(reviewFactory.create("review", "review1", new Review(1, "product2")));
        testDriver.pipeInput(productFactory.create("product", "product2", new Product("anothernother name")));
        testDriver.pipeInput(reviewFactory.create("review", "review2", new Review(2, "product2")));
        testDriver.pipeInput(reviewFactory.create("review", "review3", new Review(3, "product1")));
        testDriver.pipeInput(productFactory.create("product", "product2", new Product("final name")));
        testDriver.pipeInput(productFactory.create("product", "product1", null));
        testDriver.pipeInput(reviewFactory.create("review", "review3", null));
        testDriver.pipeInput(reviewFactory.create("review", "review4", null));
        testDriver.pipeInput(productFactory.create("product", "product4", new Product("name 4")));
        testDriver.pipeInput(reviewFactory.create("review", "review4", new Review(4, "product4")));
        testDriver.pipeInput(reviewFactory.create("review", "review5", new Review(5, "product5")));

        ProducerRecord<byte[], byte[]> result;
        do {
            result = testDriver.readOutput("joinStream");
            if (result != null) {
                System.out.println("key: " + (result.key() == null ? null : new String(result.key())) + ", value: " + (result.value() == null ? null : new String(result.value())));
            }
        } while (result != null);

    }

    private static class ReviewWithProductName {
        private int rating;
        private String productName;

        @JsonCreator
        public ReviewWithProductName(@JsonProperty("rating") int rating, @JsonProperty("productName") String productName) {
            this.rating = rating;
            this.productName = productName;
        }

        public int getRating() {
            return rating;
        }

        public void setRating(int rating) {
            this.rating = rating;
        }

        public String getProductName() {
            return productName;
        }

        public void setProductName(String productName) {
            this.productName = productName;
        }
    }

    private static class Review {
        private int rating;
        private String productId;

        @JsonCreator
        public Review(@JsonProperty("rating") int rating, @JsonProperty("productId") String productId) {
            this.rating = rating;
            this.productId = productId;
        }

        public int getRating() {
            return rating;
        }

        public String getProductId() {
            return productId;
        }

        public void setRating(int rating) {
            this.rating = rating;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }
    }

    private static class Product {
        private String name;

        @JsonCreator
        public Product(@JsonProperty("name") String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
