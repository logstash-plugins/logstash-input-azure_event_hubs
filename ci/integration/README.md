Integration Testing
--------------

Integration testing is currently a manual effort. Some tooling has been supplied to assist with this testing. 

## Manually Testing

The general pattern for manual testing is:

* Send some data to an Event Hub
* Read said data from an Event Hub using Logstash via Docker
* Verify the counts of events are as expected via the http metrics endpoint

More advanced testing can be performed by staging specific data, or by changing the configurations. See the documentation for all the configuration options available. 

Below are some simple examples.  

### Manual Testing example (no storage account)

Set up your environment for what you want to test. Note - you will need read/write permissions to an Event Hub.
```
export ELASTIC_STACK_VERSION=6.2.4
export EVENT_HUB_CONNECTION1="Endpoint=sb://... your connection string here ..."
```
Note - your connection string must contain the EntityPath (which is the Event Hub name)

Allow Logstash to read the events. Docker compose start 2 instances, and since there is not storage connection specified each instance of Logstash works independently, each reading all of the events.  

```bash
ci/integration/docker-test.sh no_storage.config
``` 

Write 1000 events to the Event Hub:
```bash
cd ci/integration/event_hub_producer
mvn package
java -jar target/event-hub-producer.jar $EVENT_HUB_CONNECTION1
cd -
```  

Verify that 1000 events were written (without Logstash)
```bash
cd ci/integration/event_hub_consumer
mvn package
java -jar target/event-hub-consumer.jar $EVENT_HUB_CONNECTION1
cd -
``` 

Verify 1000 events were written with Logstash
```bash
curl -XGET 'localhost:9600/_node/stats/pipelines?pretty'
curl -XGET 'localhost:9601/_node/stats/pipelines?pretty'
```

Compare the events in/out counts to the number of events in the Event Hub. Since there is no storage account and there are 2 instances of Logstash each instance of Logstash will have processed ALL of the events. The total between the two instances should equal 2* the number of events. 

If you stop and re-run `ci/integration/docker-test.sh no_storage.config` all events will get re-processed. 

### Manual Testing example (with storage account)

The storage account fulfills the role that Zookeeper would normally fulfill allowing the different instances of Logstash to work together, and to persist the offsets so that there are not duplicated events upon restarts. 


Set up your environment for what you want to test. Note - you will need read/write permissions to an Event Hub.
```
export ELASTIC_STACK_VERSION=6.2.4
export EVENT_HUB_CONNECTION1="Endpoint=sb://... your connection string here ..."
export STORAGE_CONNECTION1="DefaultEndpointsProtocol=https;AccountName=.... your connection string here ..."
```
Note - your Event Hub connection string must contain the EntityPath (which is the Event Hub name)

Allow Logstash to read the same events. Docker compose will start 2 instances, and since there is a storage connection specified the instances of Logstash should work as cluster, each processing some of the events. 

```bash
ci/integration/docker-test.sh with_storage.config
``` 

Write 1000 events to the Event Hub:
```bash
cd ci/integration/event_hub_producer
mvn package
java -jar target/event-hub-producer.jar $EVENT_HUB_CONNECTION1
cd -
```  

Verify that 1000 events were written (without Logstash)
```bash
cd ci/integration/event_hub_consumer
mvn package
java -jar target/event-hub-consumer.jar $EVENT_HUB_CONNECTION1
cd -
``` 

Verify 1000 events were written with Logstash
```bash
curl -XGET 'localhost:9600/_node/stats/pipelines?pretty'
curl -XGET 'localhost:9601/_node/stats/pipelines?pretty'
```

Compare the events in/out counts to the number of events in the Event Hub. Since there is a storage account and there are 2 instances of Logstash each instance of Logstash will have processed some of the events. The total between the two instances should be equal to the number of events. 

If you stop and re-run `ci/integration/docker-test.sh with_storage.config` NO events will get re-processed. 