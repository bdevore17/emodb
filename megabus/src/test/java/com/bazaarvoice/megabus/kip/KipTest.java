package com.bazaarvoice.megabus.kip;

import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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
                (review, product) -> new ReviewWithProductName(review.getRating(), product.getName()),
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

//        AdminClient adminClient = AdminClient.create(properties);
//        NewTopic reviewTopic = new NewTopic("review", 8,(short) 1);
//        adminClient.createTopics(Collections.singleton(reviewTopic));

        ConsumerRecordFactory<String, Review> factory = new ConsumerRecordFactory<>("review", new StringSerializer(), new JsonPOJOSerde<>(Review.class).serializer());
        testDriver.pipeInput(factory.create("review", "review1", new Review(5, "product1")));

    }

    private class ReviewWithProductName {
        private int rating;
        private String productName;

        public ReviewWithProductName(int rating, String productName) {
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

    private class Review {
        private int rating;
        private String productId;

        public Review(int rating, String productId) {
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

    private class Product {
        private String name;

        public Product(String name) {
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
