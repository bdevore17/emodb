package com.bazaarvoice.emodb.common.discovery;

import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates how service names are built -- generally clients and services that wish to construct
 * service names (e.g., for service discovery) should use this class, so that they're sharing the same naming
 * conventions.
 */
public class ServiceNames {

    /** Prevent instantiation. */
    private ServiceNames() {
    }

    public static String forNamespaceAndBaseServiceName(String namespace, String baseServiceName) {
        requireNonNull(baseServiceName);
        return namespace == null || namespace.isEmpty() ? baseServiceName : namespace + "-" + baseServiceName;
    }

    public static boolean isValidServiceName(String serviceName, String baseServiceName) {
        return serviceName.equals(baseServiceName) || serviceName.endsWith("-" + baseServiceName);
    }
}
