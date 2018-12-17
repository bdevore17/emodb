package com.bazaarvoice.emodb.web.throttling;


import org.glassfish.jersey.server.ContainerRequest;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Supplies a single instance of a concurrent request regulator for all requests.
 */
public class InstanceConcurrentRequestRegulatorSupplier implements ConcurrentRequestRegulatorSupplier {

    private final ConcurrentRequestRegulator _requestRegulator;

    public InstanceConcurrentRequestRegulatorSupplier(ConcurrentRequestRegulator requestRegulator) {
        _requestRegulator = checkNotNull(requestRegulator, "Request regulator is required");
    }

    @Override
    public ConcurrentRequestRegulator forRequest(ContainerRequest request) {
        return _requestRegulator;
    }
}
