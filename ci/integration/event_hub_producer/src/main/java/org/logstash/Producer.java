package org.logstash;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    /**
     * Usage:
     * cd integration/event_hub_producer
     * mvn package
     * java -jar target/event-hub-producer.jar "[your_connection_string_here]" "[demo]"
     */
    public static void main(String... args) {


        if (args.length < 1 || !args[0].startsWith("Endpoint=sb") || !args[0].contains("EntityPath")) {
            LOGGER.error("The first argument must be the event hub connection string with the EntityPath. For example:");
            LOGGER.error("Endpoint=sb://logstash-demo.servicebus.windows.net/;SharedAccessKeyName=activity-log-ro;SharedAccessKey=<redacted>;EntityPath=my_event_hub");
            System.exit(1);
        }
        boolean useDemoData = false;
        if (args.length == 2 && args[1].equalsIgnoreCase("demo")) {
            useDemoData = true;
        }

        try {
            if (useDemoData) {
                sendDemoData(args[0]);
            } else {
                sendTestData(args[0]);
            }

            LOGGER.info("DONE.");

        } catch (Throwable t) {
            LOGGER.error("Something bad just happened :(", t);
        }
    }

    private static void sendDemoData(String connectionString) throws Exception {
        LOGGER.info("Sending demo data...");
        ObjectMapper mapper = new ObjectMapper();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final EventHubClient client = EventHubClient.createSync(connectionString, executorService);
        try (ZipInputStream in = new ZipInputStream(Producer.class.getClassLoader().getResourceAsStream("demo_activity_log.zip"))) {
            ZipEntry zipEntry = in.getNextEntry();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (zipEntry != null) {
                if (!zipEntry.isDirectory() && zipEntry.getName().endsWith("json")) {
                    LOGGER.info("***** Processing " + zipEntry.getName() + " ******");
                    baos.reset();
                    int result = in.read();
                    int bytes = 0;
                    while (result != -1) {
                        baos.write((byte) result);
                        result = in.read();
                    }

                    //break up each event back into individual records and back to a smaller aggregate form to avoid 256kb limitation
                    JsonNode root = mapper.readTree(baos.toString("UTF-8"));
                    JsonNode records = root.get("records");
                    Iterator<JsonNode> it = records.elements();
                    ObjectNode parent = mapper.createObjectNode();
                    ArrayNode recordsToWrite = parent.putArray("records");
                    while (it.hasNext()) {
                        JsonNode node = it.next();
                        bytes += node.toString().length() * 8;
                        // the limit is 256kb ...but it is actually a bit less then that, keep on the safe side at 100kb
                        if (bytes <= 100000) {
                            recordsToWrite.add(node);
                        } else {
                            push(parent, client);
                            parent = mapper.createObjectNode();
                            recordsToWrite = parent.putArray("records");
                            bytes = 0;
                        }
                    }
                    push(parent, client);
                    zipEntry = in.getNextEntry();
                } else {
                    zipEntry = in.getNextEntry();
                    continue;
                }
            }
        }

    }

    private static void push(ObjectNode parent, EventHubClient client) {
        try {
            EventData sendEvent = EventData.create(parent.toString().getBytes(StandardCharsets.UTF_8));
            client.sendSync(sendEvent);
        } catch (Exception e) {
            LOGGER.error("Error pushing to Azure", e);
        }
    }

    /**
     * Sends numerically ordered events
     */
    private static void sendTestData(String connectionString) throws Exception {
        final int EVENTS_TO_SEND = 1000;
        LOGGER.info("Sending {} events ...", EVENTS_TO_SEND);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final EventHubClient client = EventHubClient.createSync(connectionString, executorService);
        try {
            for (int i = 0; i < EVENTS_TO_SEND; i++) {
                EventData sendEvent = EventData.create(Integer.toString(i).getBytes(StandardCharsets.UTF_8));
                client.sendSync(sendEvent);
            }

            LOGGER.info("Successfully sent {} events to {}", EVENTS_TO_SEND, client.getEventHubName());

        } finally {
            try {
                client.closeSync();
                executorService.shutdown();
            } catch (Exception e) {
                LOGGER.error("Exception while closing.", e);
            }
        }
    }

}
