package com.bazaarvoice.megabus.tableevents;

import com.bazaarvoice.emodb.table.db.eventregistry.TableEventRegistry;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class TableEventProcessor extends AbstractScheduledService {

    private static final Logger _log = LoggerFactory.getLogger(TableEventProcessor.class);


    private final TableEventRegistry _tableEventRegistry;
    private final MetricRegistry _metricRegistry;
    private final String _applicationId;

    @Inject
    public TableEventProcessor(@MegabusApplicationId String applicationId,
                               TableEventRegistry tableEventRegistry,
                               MetricRegistry metricRegistry) {
        _tableEventRegistry = requireNonNull(tableEventRegistry);
        _metricRegistry = requireNonNull(metricRegistry);
        _applicationId = requireNonNull(applicationId);
    }

    @Override
    protected void runOneIteration() throws Exception {
        _log.info(Optional.ofNullable(_tableEventRegistry.getNextTableEvent(_applicationId))
                .map(Object::toString)
                .orElse(null)
        );
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 5, TimeUnit.SECONDS);
    }
}
