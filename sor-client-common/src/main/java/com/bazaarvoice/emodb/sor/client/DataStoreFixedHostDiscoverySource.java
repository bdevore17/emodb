package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.discovery.ConfiguredFixedHostDiscoverySource;
import com.bazaarvoice.emodb.common.discovery.ConfiguredPayload;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;

/**
 * A SOA (Ostrich) helper class that can be used to configure a {@link com.bazaarvoice.ostrich.ServicePool}
 * with a fixed, hard-coded set of hosts, useful for testing and for cross-data center API calls where
 * the client and EmoDB servers aren't in the same data center and don't have access to the same ZooKeeper
 * ensemble.
 *
 * @see com.bazaarvoice.emodb.common.discovery.ConfiguredFixedHostDiscoverySource
 */
public class DataStoreFixedHostDiscoverySource extends ConfiguredFixedHostDiscoverySource {

    public DataStoreFixedHostDiscoverySource() {
        super();
    }

    public DataStoreFixedHostDiscoverySource(String... hosts) {
        super(hosts);
    }

    @JsonCreator
    public DataStoreFixedHostDiscoverySource(Map<String, ConfiguredPayload> endPoints) {
        super(endPoints);
    }

    @Override
    protected String getBaseServiceName() {
        return DataStoreClient.BASE_SERVICE_NAME;
    }

    @Override
    protected String getServicePath() {
        return DataStoreClient.SERVICE_PATH;
    }
}
