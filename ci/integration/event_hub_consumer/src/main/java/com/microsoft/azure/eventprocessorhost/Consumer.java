package com.microsoft.azure.eventprocessorhost;
// ^^ need to be in this package to get access to the hostcontext needed for the in-memory checkpointer

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses EPH, but only the in-memory version... so this only works as a single standalone consumer using the $Default consumer group.
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    /**
     * Usage:
     * cd integration/event_hub_consumer
     * mvn package
     * java -jar target/event-hub-consumer.jar "[your_connection_string_here]"
     */
    public static void main(String... args) {

        try {
            if (args.length != 1 || !args[0].startsWith("Endpoint=sb") || !args[0].contains("EntityPath")) {
                LOGGER.error("The first and only argument must be the event hub connection string with the EntityPath. For example:");
                LOGGER.error("Endpoint=sb://logstash-demo.servicebus.windows.net/;SharedAccessKeyName=activity-log-ro;SharedAccessKey=<redacted>;EntityPath=my_event_hub");
                System.exit(1);
            }
            String eventHubName = new ConnectionStringBuilder(args[0]).getEventHubName();
            LOGGER.debug("Consuming events from Event Hub {} ...", eventHubName);
            InMemoryCheckpointManager inMemoryCheckpointManager = new InMemoryCheckpointManager();
            InMemoryLeaseManager inMemoryLeaseManager = new InMemoryLeaseManager();
            EventProcessorHost host = new EventProcessorHost(
                    EventProcessorHost.createHostName("logstash"),
                    eventHubName,
                    "$Default",
                    args[0],
                    inMemoryCheckpointManager,
                    inMemoryLeaseManager);

            inMemoryCheckpointManager.initialize(host.getHostContext());
            inMemoryLeaseManager.initialize(host.getHostContext());
            LOGGER.debug("Registering host named {}", host.getHostName());
            EventProcessorOptions options = new EventProcessorOptions();
            options.setExceptionNotification(new ErrorNotificationHandler());

            host.registerEventProcessor(EventProcessor.class, options)
                    .whenComplete((unused, e) ->
                    {
                        if (e != null) {
                            LOGGER.error("Failure while registering", e);
                            if (e.getCause() != null) {
                                LOGGER.error("Inner exception: {}", e.getCause().toString());
                            }
                        }
                    })
                    .thenAccept((unused) ->
                    {
                        System.out.println("Press enter to stop !");
                        try {
                            System.in.read();
                        } catch (Exception e) {
                            LOGGER.error("Keyboard read failed", e);
                        }
                    })
                    .thenCompose((unused) ->
                    {
                        return host.unregisterEventProcessor();
                    })
                    .exceptionally((e) ->
                    {
                        LOGGER.error("Failure while unregistering", e);
                        if (e.getCause() != null) {
                            LOGGER.error("Inner exception: {} ", e.getCause().toString());
                        }
                        return null;
                    })
                    .get(); // Wait for everything to finish before exiting main! This takes a while :(

            LOGGER.info("Done reading. Thanks for playing. ");
        } catch (Throwable t) {
            LOGGER.error("Something bad just happened :(", t);
        }
    }

}
