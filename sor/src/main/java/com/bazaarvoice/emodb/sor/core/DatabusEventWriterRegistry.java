package com.bazaarvoice.emodb.sor.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is used to be used to send events to the databus prior to writing to the system of record.
 * This databus should register as an {@link DatabusEventWriter} with this registry and then take events synchronously.
 * If an event fails to be written to the databus, then it should propagate and exception and prevent Emo from writing
 * to the system of record.
 */
public class DatabusEventWriterRegistry {

    private DatabusEventWriter _databusWriter;
    private boolean _hasRegistered;

    public DatabusEventWriterRegistry() {
        _hasRegistered = false;
        _databusWriter = event -> {};
    }

    public void registerDatabusEventWriter(DatabusEventWriter databusWriter) {
        checkNotNull(databusWriter);
        checkArgument(!_hasRegistered);
        _databusWriter = databusWriter;
        _hasRegistered = true;
    }

    public DatabusEventWriter getDatabusWriter() {
        return _databusWriter;
    }
}
