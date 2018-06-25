package com.bazaarvoice.emodb.sor.audit;

import com.bazaarvoice.emodb.sor.api.Audit;

public class DiscardingAuditWriter implements AuditWriter {
    @Override
    public void persist(String table, String key, Audit audit, long auditTime) {
        // Discard
    }
}
