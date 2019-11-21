package com.bazaarvoice.emodb.table.db.eventregistry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import static com.google.common.base.Preconditions.checkNotNull;

public class TableEvent {

    private enum TableOperation {
        DROP,
        DROP_FROM_MOVE,
        PROMOTE_FROM_MOVE,
        TEMPLATE_CHANGE
    }

    private final String _eventKey;
    private final TableOperation _tableOperation;
    private final String _oldPlacement;
    private final String _newPlacement;
    private final boolean _completed;
    private final long _uuid;
    private final String _table;


    public TableEvent(String eventKey, TableOperation tableOperation, String oldPlacement, String newPlacement,
                      long uuid, String table) {
        this(eventKey, tableOperation, oldPlacement, newPlacement, false, uuid, table);
    }

    @JsonCreator
    private TableEvent(@JsonProperty("eventKey") String eventKey,
                      @JsonProperty("tableOperation") TableOperation tableOperation,
                      @JsonProperty("oldPlacement") String oldPlacement,
                      @JsonProperty("newPlacement") String newPlacement,
                      @JsonProperty("completed") boolean completed,
                      @JsonProperty("uuid") long uuid,
                      @JsonProperty("table") String table) {
        _eventKey = checkNotNull(eventKey);
        _tableOperation = checkNotNull(tableOperation);
        _oldPlacement = checkNotNull(oldPlacement);
        _newPlacement = checkNotNull(newPlacement);
        _completed = completed;
        _uuid = uuid;
        _table = table;
    }

    public String getEventKey() {
        return _eventKey;
    }

    public TableOperation getTableOperation() {
        return _tableOperation;
    }

    public String getOldPlacement() {
        return _oldPlacement;
    }

    public String getNewPlacement() {
        return _newPlacement;
    }

    public long getUuid() {
        return _uuid;
    }

    public String getTable() {
        return _table;
    }

    public boolean isCompleted() {
        return _completed;
    }
}
