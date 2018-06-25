package com.bazaarvoice.emodb.sor;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Size;
import org.joda.time.Duration;

import javax.annotation.Nonnull;

public class AuditLogConfiguration {

    @JsonProperty("logBucket")
    @Nonnull
    private String _logBucket;

    @JsonProperty("logBucketRegion")
    @Nonnull
    private String _logBucketRegion = "us-east-1";

    @JsonProperty("logPath")
    @Nonnull
    private String _logPath;

    /* Only required if not using default credentials provider */
    @JsonProperty("s3AccessKey")
    private String _s3AccessKey;

    /* Only required if not using default credentials provider */
    @JsonProperty("s3SecretKey")
    private String _s3SecretKey;

    @JsonProperty("maxFileSize")
    private long _maxFileSize = Size.megabytes(50).toBytes();

    @JsonProperty("maxBatchTime")
    private Duration _maxBatchTime = Duration.standardMinutes(2);

    @JsonProperty("stagingDir")
    private String _stagingDir;

    @JsonProperty("logFilePrefix")
    private String _logFilePrefix = "audit-log";

    /* Useful for local testing, impractical to disable in production */
    @JsonProperty("fileTransfersEnabled")
    private boolean _fileTransfersEnabled = true;

    @Nonnull
    public String getLogBucket() {
        return _logBucket;
    }

    public AuditLogConfiguration setLogBucket(@Nonnull String logBucket) {
        _logBucket = logBucket;
        return this;
    }

    @Nonnull
    public String getLogBucketRegion() {
        return _logBucketRegion;
    }

    public AuditLogConfiguration setLogBucketRegion(@Nonnull String logBucketRegion) {
        _logBucketRegion = logBucketRegion;
        return this;
    }

    @Nonnull
    public String getLogPath() {
        return _logPath;
    }

    public AuditLogConfiguration setLogPath(@Nonnull String logPath) {
        _logPath = logPath;
        return this;
    }

    public String getS3AccessKey() {
        return _s3AccessKey;
    }

    public AuditLogConfiguration setS3AccessKey(String s3AccessKey) {
        _s3AccessKey = s3AccessKey;
        return this;
    }

    public String getS3SecretKey() {
        return _s3SecretKey;
    }

    public AuditLogConfiguration setS3SecretKey(String s3SecretKey) {
        _s3SecretKey = s3SecretKey;
        return this;
    }

    public long getMaxFileSize() {
        return _maxFileSize;
    }

    public AuditLogConfiguration setMaxFileSize(long maxFileSize) {
        _maxFileSize = maxFileSize;
        return this;
    }

    public Duration getMaxBatchTime() {
        return _maxBatchTime;
    }

    public AuditLogConfiguration setMaxBatchTime(Duration maxBatchTime) {
        _maxBatchTime = maxBatchTime;
        return this;
    }

    public String getStagingDir() {
        return _stagingDir;
    }

    public AuditLogConfiguration setStagingDir(String stagingDir) {
        _stagingDir = stagingDir;
        return this;
    }

    public String getLogFilePrefix() {
        return _logFilePrefix;
    }

    public AuditLogConfiguration setLogFilePrefix(String logFilePrefix) {
        _logFilePrefix = logFilePrefix;
        return this;
    }

    public boolean isFileTransfersEnabled() {
        return _fileTransfersEnabled;
    }

    public AuditLogConfiguration setFileTransfersEnabled(boolean fileTransfersEnabled) {
        _fileTransfersEnabled = fileTransfersEnabled;
        return this;
    }
}
