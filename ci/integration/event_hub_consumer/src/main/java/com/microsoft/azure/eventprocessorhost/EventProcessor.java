package com.microsoft.azure.eventprocessorhost;

import com.microsoft.azure.eventhubs.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class EventProcessor implements IEventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);
    private static final AtomicLong totalCount = new AtomicLong();

    @Override
    public void onOpen(PartitionContext context) throws Exception {
        LOGGER.debug("Partition {} is opening", context.getPartitionId());
    }

    @Override
    public void onClose(PartitionContext context, CloseReason reason) throws Exception {
        LOGGER.debug("Partition {} is closing for reason {} ", context.getPartitionId(), reason.toString());
    }

    @Override
    public void onError(PartitionContext context, Throwable error) {
        LOGGER.error("Partition {}", context.getPartitionId(), error);
    }

    @Override
    public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception {
        LOGGER.debug("Partition {} got event batch", context.getPartitionId());
        int eventCount = 0;
        for (EventData data : events) {

            try {
                LOGGER.debug("Received event: {}", new String(data.getBytes(), StandardCharsets.UTF_8));

                eventCount++;
                totalCount.incrementAndGet();
                // not check pointing here on purpose
            } catch (Exception e) {
                LOGGER.error("Processing failed for an event ", e);
            }
        }
        LOGGER.debug("Partition {} processed a batch of size {} for host {} ", context.getPartitionId(), eventCount, context.getOwner());
        LOGGER.info("************* Consumed {} total events (so far) **********", totalCount);
    }
}
