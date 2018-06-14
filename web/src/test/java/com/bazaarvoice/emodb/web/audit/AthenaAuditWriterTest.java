package com.bazaarvoice.emodb.web.audit;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Stopwatch;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.util.Size;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

public class AthenaAuditWriterTest {

    @Test
    public void testWriter() throws Exception {
        AmazonS3 s3 = mock(AmazonS3.class);
        String bucket = "test-bucket";
        String prefix = "emodb-audit";
        long maxFileSize = Size.megabytes(1).toBytes();
        Duration maxBatchTime = Duration.ofSeconds(10);
        File stagingDir = new File("/Users/bill.kalter/tmp/athenalogs");
        Clock clock = Clock.systemUTC();

        AthenaAuditWriter writer = new AthenaAuditWriter(s3, URI.create("s3://test-bucket/path/to/audits"), maxFileSize,
                maxBatchTime, stagingDir, prefix, Jackson.newObjectMapper(), clock);

        String table = "test:table";
        long keyId = 0;

        writer.start();
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            while (stopwatch.elapsed(TimeUnit.MINUTES) < 1) {
                String key = "doc-" + keyId++;
                Audit audit = new AuditBuilder().setLocalHost().setComment("testing " + keyId).setProgram("testng").build();
                writer.persist(table, key, audit, clock.millis());
            }
        } finally {
            writer.stop();
        }
    }

    @Test
    public void testJackson() throws Exception {
        ObjectMapper objectMapper = Jackson.newObjectMapper();
        ObjectWriter writer = objectMapper.writer();
        System.out.println(writer.getConfig().getSerializationFeatures());
        writer = writer.with(SerializationFeature.CLOSE_CLOSEABLE);
        System.out.println(writer.getConfig().getSerializationFeatures());

    }
}
