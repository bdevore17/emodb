package com.bazaarvoice.emodb.sor.audit;

import com.bazaarvoice.emodb.sor.api.Audit;

public interface AuditWriter {

    void persist(String table, String key, Audit audit, long auditTime);
}
