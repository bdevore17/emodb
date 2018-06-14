package com.bazaarvoice.emodb.web.audit;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.audit.AuditWriter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Size;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AthenaAuditWriter implements AuditWriter, Managed {

    private final static Logger _log = LoggerFactory.getLogger(AthenaAuditWriter.class);

    private final static DateTimeFormatter LOG_FILE_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);
    
    private final static long DEFAULT_MAX_FILE_SIZE = Size.megabytes(10).toBytes();
    private final static Duration DEFAULT_MAX_BATCH_TIME = Duration.ofMinutes(2);

    private final AmazonS3 _s3;
    private final String _s3Bucket;
    private final String _s3AuditRoot;
    private final long _maxFileSize;
    private final long _maxBatchTimeMs;
    private final File _stagingDir;
    private final String _logFilePrefix;
    private final BlockingQueue<QueuedAudit> _auditQueue;
    private final Clock _clock;
    private final ObjectWriter _objectWriter;
    private final ConcurrentMap<Long, AuditOutput> _openAuditOutputs = Maps.newConcurrentMap();
    
    private ScheduledExecutorService _service;
    private AuditOutput _mruAuditOutput;
    
    public AthenaAuditWriter(AmazonS3 s3, URI s3AuditRootUri, long maxFileSize, Duration maxBatchTime,
                             File stagingDir, String logFilePrefix, ObjectMapper objectMapper, Clock clock) {
        _s3 = requireNonNull(s3, "s3");
        requireNonNull(s3AuditRootUri, "s3AuditRoot");
        checkArgument("s3".equals(s3AuditRootUri.getScheme()), "Audit root must be in s3");
        _s3Bucket = s3AuditRootUri.getHost();
        checkArgument(!Strings.isNullOrEmpty(_s3Bucket), "S3 bucket is required");

        String s3AuditRoot = s3AuditRootUri.getPath();
        if (s3AuditRoot.startsWith("/")) {
            s3AuditRoot = s3AuditRoot.substring(1);
        }
        if (!s3AuditRoot.endsWith("/")) {
            s3AuditRoot = s3AuditRoot + "/";
        }
        _s3AuditRoot = s3AuditRoot;

        checkArgument(stagingDir.exists(), "Staging directory must exist");

        _maxFileSize = maxFileSize > 0 ? maxFileSize : DEFAULT_MAX_FILE_SIZE;
        _maxBatchTimeMs = (maxBatchTime != null && maxBatchTime.compareTo(Duration.ZERO) > 0 ? maxBatchTime : DEFAULT_MAX_BATCH_TIME).toMillis();
        _stagingDir = requireNonNull(stagingDir, "stagingDir");
        _logFilePrefix = requireNonNull(logFilePrefix, "logFilePrefix");
        _clock = requireNonNull(clock, "clock");

        _auditQueue = new ArrayBlockingQueue<>(4096);

        _objectWriter = objectMapper.copy()
                .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
                .writer();
    }

    @Override
    public void start() {
        long now = _clock.millis();
        long msToNextBatch = _maxBatchTimeMs - (now % _maxBatchTimeMs);

        _service = Executors.newScheduledThreadPool(3, new ThreadFactoryBuilder().setNameFormat("audit-log-%d").build());

        _service.scheduleAtFixedRate(this::doLogFileMaintenance,
                msToNextBatch, _maxBatchTimeMs, TimeUnit.MILLISECONDS);

        _service.submit(this::processQueuedAudits);
    }

    @Override
    public void stop() throws Exception {
        _service.shutdown();
        if (!_service.awaitTermination(30, TimeUnit.SECONDS)) {
            _log.warn("Audits still processing unexpectedly after shutdown");
        }
        // Close all complete log files.  Don't transfer them now since we're shutting down; the next time the service
        // starts they'll be transferred.
        closeCompleteLogFiles(true);
    }

    @Override
    public void persist(String table, String key, Audit audit, long auditTime) {
        try {
            _auditQueue.put(new QueuedAudit(table, key, audit, auditTime));
        } catch (InterruptedException e) {
            _log.warn("Interrupted attempting to write audit for {}/{}", table, key);
        }
    }

    private void doLogFileMaintenance() {
        // Close all files that have either exceeded their maximum size or age but have not closed due to lack of
        // audit activity.
        closeCompleteLogFiles(false);

        // Find all closed log files in the staging directory and move them to S3
        for (File logFile : _stagingDir.listFiles((dir, name) -> name.startsWith(_logFilePrefix) && name.endsWith(".log.gz")))           {
            String auditDate = logFile.getName().substring(_logFilePrefix.length() + 1, _logFilePrefix.length() + 9);

//            if (!logFile.delete()) {
//                _log.warn("Failed to delete file after copying to s3: {}", logFile);
//            }
        }
    }

    private void closeCompleteLogFiles(boolean forceClose) {
        for (AuditOutput auditOutput : ImmutableList.copyOf(_openAuditOutputs.values())) {
            if (forceClose || auditOutput.shouldClose()) {
                auditOutput.close();
            }
        }
    }

    private void processQueuedAudits() {
        QueuedAudit audit;
        try {
            while (!_service.isShutdown()) {
                if ((audit = _auditQueue.poll(1, TimeUnit.SECONDS)) != null) {
                    boolean written = false;
                    while (!written) {
                        AuditOutput auditOutput = getAuditOutputForTime(audit.time);
                        written = auditOutput.writeAudit(audit);
                    }
                }
            }
        } catch (InterruptedException e) {
            if (!_service.isShutdown()) {
                _log.warn("Audit log service interrupted while service is still running");
            }
        } catch (Exception e) {
            _log.error("Processing of queued audits failed", e);
        } finally {
            // On any exception so long as the service is still running restart processing queued audits
            if (!_service.isShutdown()) {
                _service.schedule(this::processQueuedAudits, 1, TimeUnit.SECONDS);
            }
        }
    }

    private AuditOutput getAuditOutputForTime(long time) {
        long batchTime = time - (time % _maxBatchTimeMs);
        AuditOutput mruAuditOutput = _mruAuditOutput;
        if (mruAuditOutput != null && batchTime == mruAuditOutput.getBatchTime() && !mruAuditOutput.isClosed()) {
            return mruAuditOutput;
        }

        return _mruAuditOutput = _openAuditOutputs.computeIfAbsent(batchTime, this::createNewAuditLogOut);
    }

    private AuditOutput createNewAuditLogOut(long batchTime) {
        try {
            long now = _clock.millis();
            long nextBatchCycleCloseTime = now - (now % _maxBatchTimeMs) + _maxBatchTimeMs;

            return new AuditOutput(LOG_FILE_DATE_FORMATTER.format(Instant.ofEpochMilli(batchTime)), batchTime, nextBatchCycleCloseTime);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create new AuditOutput file");
        }
    }

    private static class QueuedAudit {
        final String table;
        final String key;
        final Audit audit;
        final long time;

        QueuedAudit(String table, String key, Audit audit, long time) {
            this.table = table;
            this.key = key;
            this.audit = audit;
            this.time = time;
        }
    }

    private class AuditOutput {
        private final File _auditLogFile;
        private final long _batchTime;
        private final long _closeTime;
        private final ReentrantLock _lock = new ReentrantLock();
        private volatile boolean _closed;
        private volatile int _auditsWritten = 0;
        private volatile OutputStream _auditLogOut;
        private volatile CountingOutputStream _bytesWrittenOut;


        AuditOutput(String datePrefix, long batchTime, long closeTime) throws IOException {
            String fileName = String.format("%s.%s.%s.log.gz.tmp", _logFilePrefix, datePrefix, UUID.randomUUID());
            _auditLogFile = new File(_stagingDir, fileName);
            _batchTime = batchTime;
            _closeTime = closeTime;
        }

        void createAuditLogOut() throws IOException {
            FileOutputStream fileOut = new FileOutputStream(_auditLogFile);
            _bytesWrittenOut = new CountingOutputStream(fileOut);
            _auditLogOut = new GzipCompressorOutputStream(_bytesWrittenOut);
        }

        boolean writeAudit(QueuedAudit audit) {
            Map<String, Object> auditMap = Maps.newLinkedHashMap();
            // This is an intentional break from convention to use "tablename" instead of "table".  This is because
            // "table" is a reserved word in Presto and complicates queries for that column.
            auditMap.put("tablename", audit.table);
            auditMap.put("key", audit.key);
            auditMap.put("time", audit.time);
            // Even though the content of the audit is valid JSON the potential key set is unbounded.  This makes
            // it difficult to define a schema for Presto.  So create values for the conventional keys and store
            // the rest in an opaque blob.
            Map<String, Object> custom = new HashMap<>(audit.audit.getAll());
            if (custom.remove(Audit.COMMENT) != null) {
                auditMap.put("comment", audit.audit.getComment());
            }
            if (custom.remove(Audit.HOST) != null) {
                auditMap.put("host", audit.audit.getHost());
            }
            if (custom.remove(Audit.PROGRAM) != null) {
                auditMap.put("program", audit.audit.getProgram());
            }
            if (custom.remove(Audit.TAGS) != null) {
                auditMap.put("tags", audit.audit.getTags());
            }
            if (!custom.isEmpty()) {
                auditMap.put("custom", JsonHelper.asJson(custom));
            }

            _lock.lock();
            try {
                if (shouldClose()) {
                    close();
                }

                if (isClosed()) {
                    return false;
                }

                if (_auditLogOut == null) {
                    createAuditLogOut();
                }

                _objectWriter.writeValue(_auditLogOut, auditMap);
                _auditLogOut.write('\n');
                //noinspection NonAtomicOperationOnVolatileField
                _auditsWritten += 1;
            } catch (IOException e) {
                _log.warn("Failed to write audit to logs", e);
            } finally {
                _lock.unlock();
            }

            return true;
        }

        boolean isClosed() {
            return _closed;
        }
        
        void close() {
            _lock.lock();
            try {
                if (!_closed) {
                    _closed = true;

                    if (_auditLogOut != null) {
                        _auditLogOut.close();
                    }

                    if (_auditsWritten != 0) {
                        // Move the file to a new file without the ".tmp" suffix
                        String tmpFileName = _auditLogFile.getName();
                        String finalFileName = tmpFileName.substring(0, tmpFileName.length() - 4);
                        Files.move(_auditLogFile.toPath(), new File(_auditLogFile.getParentFile(), finalFileName).toPath());
                    }
                }
            } catch (IOException e) {
                _log.warn("Failed to close log file", e);
            } finally {
                _openAuditOutputs.remove(_batchTime, this);
                _lock.unlock();
            }
        }

        long getBatchTime() {
            return _batchTime;
        }
        
        boolean isBatchTimedOut() {
            return _clock.millis() >= _closeTime;
        }

        boolean isOversized() {
            return _bytesWrittenOut != null && _bytesWrittenOut.getCount() > _maxFileSize;
        }

        boolean shouldClose() {
            return isBatchTimedOut() || isOversized();
        }
    }
}
