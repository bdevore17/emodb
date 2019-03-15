package com.bazaarvoice.emodb.blob.core;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultBlob;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.Names;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.StreamSupplier;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.google.common.base.Objects;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import jersey.repackaged.com.google.common.base.Throwables;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class S3BlobStore implements BlobStore {

    private final TableDAO _tableDao;
    private final AmazonS3 _s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

    @Inject
    public S3BlobStore(TableDAO tableDAO) {
        _tableDao = tableDAO;
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");

        LimitCounter remaining = new LimitCounter(limit);
        final Iterator<com.bazaarvoice.emodb.table.db.Table> tableIter = _tableDao.list(fromTableExclusive, remaining);
        return remaining.limit(new AbstractIterator<Table>() {
            @Override
            protected com.bazaarvoice.emodb.blob.api.Table computeNext() {
                while (tableIter.hasNext()) {
                    com.bazaarvoice.emodb.table.db.Table table = tableIter.next();
                    if (!table.isInternal()) {
                        return toDefaultTable(table);
                    }
                }
                return endOfData();
            }
        });
    }

    private com.bazaarvoice.emodb.blob.api.Table toDefaultTable(com.bazaarvoice.emodb.table.db.Table table) {
        //noinspection unchecked
        Map<String, String> attributes = (Map) table.getAttributes();
        return new DefaultTable(table.getName(), table.getOptions(), attributes, table.getAvailability());
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit) throws TableExistsException {
        checkLegalTableName(table);
        checkNotNull(options, "options");
        checkNotNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        checkNotNull(audit, "audit");
        _tableDao.create(table, options, attributes, audit);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        checkLegalTableName(table);
        checkNotNull(audit, "audit");
        _tableDao.drop(table, audit);
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {

    }

    @Override
    public boolean getTableExists(String table) {
        checkLegalTableName(table);
        return _tableDao.exists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        checkLegalTableName(table);
        return _tableDao.get(table).getAvailability() != null;
    }

    @Override
    public com.bazaarvoice.emodb.blob.api.Table getTableMetadata(String table) {
        checkLegalTableName(table);
        return toDefaultTable(_tableDao.get(table));
    }

    @Override
    public Map<String, String> getTableAttributes(String table) throws UnknownTableException {
        checkLegalTableName(table);
        return getAttributes(_tableDao.get(table));
    }

    @Override
    public void setTableAttributes(String table, Map<String, String> attributes, Audit audit) throws UnknownTableException {
        checkLegalTableName(table);
        checkNotNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        checkNotNull(audit, "audit");
        _tableDao.setAttributes(table, attributes, audit);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        checkLegalTableName(table);
        return _tableDao.get(table).getOptions();
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        return 0;
    }

    @Override
    public BlobMetadata getMetadata(String tableName, String blobId) throws BlobNotFoundException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        String path = UriBuilder.fromPath("{arg1}").path("{arg2}").build(table, blobId).toASCIIString();

        ObjectMetadata metadata;

        try {
            metadata = _s3.getObjectMetadata("bv-emodb-local-audit", path);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                metadata = null;
            } else {
                throw Throwables.propagate(e);
            }
        }

        return newMetadata(table, blobId, metadata);
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String tableName, @Nullable String fromBlobIdExclusive, long limit) {
        checkLegalTableName(tableName);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        Iterator<S3ObjectSummary> summaryIterator = new AbstractIterator<S3ObjectSummary>() {
            int index;
            List<S3ObjectSummary> localSummaries = null;
            int remaining = (int) limit;
            String continuationToken = null;

            @Override
            protected S3ObjectSummary computeNext() {

                if (remaining == 0) {
                    return endOfData();
                }

                if (localSummaries == null || index == localSummaries.size()) {

                    ListObjectsV2Request request = new ListObjectsV2Request()
                            .withBucketName("bv-emodb-local-audit")
                            .withPrefix(tableName)
                            .withStartAfter(fromBlobIdExclusive)
                            .withMaxKeys(Math.min(remaining, 1000));
                    if (continuationToken != null) {
                        request.setContinuationToken(continuationToken);
                    }

                    ListObjectsV2Result result = _s3.listObjectsV2(request);
                    localSummaries = result.getObjectSummaries();
                    continuationToken = result.getNextContinuationToken();
                    if (continuationToken == null) {
                        remaining = localSummaries.size();
                    }

                    index = 0;
                }

                remaining--;
                return index < localSummaries.size() ? localSummaries.get(index++) : endOfData();
            }
        };

        return new MetadataIterator(summaryIterator, table);
    }

    private class MetadataIterator extends AbstractIterator<BlobMetadata> {
        private final Iterator<S3ObjectSummary> _summaryIterator;
        private final com.bazaarvoice.emodb.table.db.Table _table;

        public MetadataIterator(Iterator<S3ObjectSummary> summaryIterator, com.bazaarvoice.emodb.table.db.Table table) {
            _summaryIterator = checkNotNull(summaryIterator);
            _table = checkNotNull(table);
        }

        @Override
        protected BlobMetadata computeNext() {
            if (_summaryIterator.hasNext()) {
                S3ObjectSummary summary = _summaryIterator.next();
                return newMetadata(_table, summary.getKey().substring(summary.getKey().lastIndexOf('/') + 1), _s3.getObjectMetadata(summary.getBucketName(), summary.getKey()));
            }
            return endOfData();
        }
    }

    @Override
    public Blob get(String table, String blobId) throws BlobNotFoundException {
        return get(table, blobId, null);
    }

    @Override
    public Blob get(String tableName, String blobId, @Nullable RangeSpecification rangeSpec) throws BlobNotFoundException, RangeNotSatisfiableException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        final com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        String path = UriBuilder.fromPath("{arg1}").path("{arg2}").build(table, blobId).toASCIIString();

        BlobMetadata metadata = getMetadata(tableName, blobId);

        final Range range;
        if (rangeSpec != null) {
            range = rangeSpec.getRange(metadata.getLength());
            // Satisfiable range requests must return at least one byte (per HTTP spec).
            checkArgument(range.getOffset() >= 0 && range.getLength() > 0 &&
                    range.getOffset() + range.getLength() <= metadata.getLength(), "Invalid byte range: %s", rangeSpec);
        } else {
            // If no range is specified, return the entire entity.  This may return zero bytes.
            range = new Range(0, metadata.getLength());
        }

        GetObjectRequest rangeObjectRequest = new GetObjectRequest("bv-emodb-local-audit", path)
               .withRange(range.getOffset(), range.getOffset() + range.getLength());

        S3Object object = _s3.getObject(rangeObjectRequest);
        S3ObjectInputStream objectInputStream = object.getObjectContent();

        return new DefaultBlob(metadata, range, new StreamSupplier() {
            @Override
            public void writeTo(OutputStream out) throws IOException {
                IOUtils.copy(objectInputStream, out);
            }
        });
    }

    private BlobMetadata newMetadata(com.bazaarvoice.emodb.table.db.Table table, String blobId, ObjectMetadata s) {
        if (s == null) {
            throw new BlobNotFoundException(blobId);
        }
        Map<String, String> attributes = Maps.newTreeMap();
        attributes.putAll(s.getUserMetadata());
        attributes.putAll(getAttributes(table));
        Date timestamp = s.getLastModified(); // Convert from microseconds
        return new DefaultBlobMetadata(blobId, timestamp, s.getContentLength(), s.getContentMD5(), s.getETag(), attributes);
    }

    @Override
    public void put(String tableName, String blobId, Supplier<? extends InputStream> in, Map<String, String> attributes, @Nullable Duration ttl) throws IOException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        checkNotNull(in, "in");
        checkNotNull(attributes, "attributes");

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        long timestamp = System.currentTimeMillis();
        String path = UriBuilder.fromPath("{arg1}").path("{arg2}").build(table, blobId).toASCIIString();

        // TODO: using the s3 api that we are currently using, we are unable to set teh md5 hash on the objectMetadata like we need to
//        DigestInputStream md5In = new DigestInputStream(in.get(), getMessageDigest("MD5"));
//        DigestInputStream sha1In = new DigestInputStream(md5In, getMessageDigest("SHA-1"));

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setUserMetadata(attributes);

        _s3.putObject("bv-emodb-local-audit", path, in.get(), objectMetadata);
    }

    @Override
    public void delete(String tableName, String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        String path = UriBuilder.fromPath("{arg1}").path("{arg2}").build(table, blobId).toASCIIString();

        _s3.deleteObject("bv-emodb-local-audit", path);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return null;
    }

    private Map<String, String> getAttributes(com.bazaarvoice.emodb.table.db.Table table) {
        // Coerce Map<String, Object> to Map<String, String>
        return (Map) table.getAttributes();
    }

    private void checkMapOfStrings(Map<?, ?> map, String message) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            checkArgument(entry.getKey() instanceof String, message);
            checkArgument(entry.getValue() instanceof String, message);
        }
    }

    private static MessageDigest getMessageDigest(String algorithmName) {
        try {
            return MessageDigest.getInstance(algorithmName);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void checkLegalTableName(String table) {
        checkArgument(Names.isLegalTableName(table),
                "Table name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'photo:testcustomer'.");
    }

    private void checkLegalBlobId(String blobId) {
        checkArgument(Names.isLegalBlobId(blobId),
                "Blob IDs must be ASCII strings between 1 and 255 characters in length. " +
                        "Whitespace, ISO control characters and certain punctuation characters that aren't generally allowed in file names are excluded.");
    }
}
