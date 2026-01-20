# encoding: utf-8
require "logstash-input-azure_event_hubs"
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "logstash/inputs/processor_factory"
require "logstash/inputs/error_notification_handler"
require "logstash/inputs/named_thread_factory"
require "logstash/inputs/look_back_position_provider"

class LogStash::Inputs::AzureEventHubs < LogStash::Inputs::Base

  java_import com.microsoft.azure.eventprocessorhost.EventProcessorHost
  java_import com.microsoft.azure.eventprocessorhost.EventProcessorOptions
  java_import com.microsoft.azure.eventprocessorhost.InMemoryCheckpointManager
  java_import com.microsoft.azure.eventprocessorhost.InMemoryLeaseManager
  java_import com.microsoft.azure.eventprocessorhost.HostContext
  java_import com.microsoft.azure.eventhubs.ConnectionStringBuilder
  java_import com.microsoft.azure.eventhubs.EventHubClient
  java_import com.microsoft.azure.eventhubs.EventHubClientOptions
  java_import com.microsoft.azure.eventhubs.EventPosition
  java_import com.microsoft.azure.eventhubs.PartitionReceiver
  java_import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider
  java_import com.microsoft.aad.adal4j.AuthenticationContext
  java_import com.microsoft.aad.adal4j.ClientCredential
  java_import java.util.concurrent.Executors
  java_import java.util.concurrent.TimeUnit
  java_import java.util.concurrent.CompletableFuture
  java_import java.time.Duration
  java_import java.net.URI

  config_name "azure_event_hubs"

  # This plugin supports two styles of configuration
  # basic - You supply a list of Event Hub connection strings complete with the 'EntityPath' that defines the Event Hub name. All other configuration is shared.
  # advanced - You supply a list of Event Hub names, and under each name provide that Event Hub's configuration. Most all of the configuration options are identical as the basic model, except they are configured per Event Hub.
  # Defaults to basic
  # Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"  , "Endpoint=sb://example2...;EntityPath=event_hub_name2"  ]
  # }
  config :config_mode, :validate => ['basic', 'advanced'], :default => 'basic'

  # advanced MODE ONLY - The event hubs to read from. This is a array of hashes, where the each entry of the array is a hash of the event_hub_name => {configuration}.
  # Note - most basic configuration options are supported under the Event Hub names, and examples proved where applicable
  # Note - while in advanced mode, if any basic options are defined at the top level they will be used if not already defined under the Event Hub name.  e.g. you may define shared configuration at the top level
  # Note - the required event_hub_connection is named 'event_hub_connection' (singular) which differs from the basic configuration option 'event_hub_connections' (plural)
  # Note - the 'event_hub_connection' may contain the 'EntityPath', but only if it matches the Event Hub name.
  # Note - the same Event Hub name is allowed under different configurations (and is why the config is array of Hashes)
  # Example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #       }},
  #       { "event_hub_name2" => {
  #           event_hub_connection => "Endpoint=sb://example2..."
  #           storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #           storage_container => "my_container"
  #      }}
  #    ]
  #    consumer_group => "logstash" # shared across all Event Hubs
  # }
  config :event_hubs, :validate => :array, :required => true # only required for advanced mode

  # basic MODE ONLY - The Event Hubs to read from. This is a list of Event Hub connection strings that includes the 'EntityPath'.
  # All other configuration options will be shared between Event Hubs.
  # Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"  , "Endpoint=sb://example2...;EntityPath=event_hub_name2"  ]
  # }
  config :event_hub_connections, :validate => :array, :required => true # only required for basic mode

  # Used to persists the offsets between restarts and ensure that multiple instances of Logstash process different partitions
  # This is *stongly* encouraged to be set for production environments.
  # When this value is set, restarts will pick up from where it left off. Without this value set the initial_position is *always* used.
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #       }}
  #    ]
  # }
  config :storage_connection, :validate => :password, :required => false

  # The storage container to persist the offsets.
  # Note - don't allow multiple Event Hubs to write to the same container with the same consumer group, else the offsets will be persisted incorrectly.
  # Note - this will default to the event hub name if not defined
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #    storage_container => "my_container"
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           storage_connection => "DefaultEndpointsProtocol=https;AccountName=example...."
  #           storage_container => "my_container"
  #       }}
  #    ]
  # }
  config :storage_container, :validate => :string, :required => false

  # Total threads used process events. Requires at minimum 2 threads. This option can not be set per Event Hub.
  # azure_event_hubs {
  #    threads => 16
  # }
  config :threads, :validate => :number, :default => 16

  # Consumer group used to read the Event Hub(s). It is recommended to change from the $Default to a consumer group specifically for Logstash, and ensure that all instances of Logstash use that consumer group.
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    consumer_group => "logstash"
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           consumer_group => "logstash"
  #       }}
  #    ]
  # }
  config :consumer_group, :validate => :string, :default => '$Default'

  # The max size of events are processed together. A checkpoint is created after each batch. Increasing this value may help with performance, but requires more memory.
  # Defaults to 50
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    max_batch_size => 125
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           max_batch_size => 125
  #       }}
  #    ]
  # }
  config :max_batch_size, :validate => :number, :default => 125

  # The max size of events that are retrieved prior to processing. Increasing this value may help with performance, but requires more memory.
  # Defaults to 300
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    prefetch_count => 300
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           prefetch_count => 300
  #       }}
  #    ]
  # }
  # NOTE - This option is intentionally not part of the public documentation. This is a very low level configuration that shouldn't need to be changed by anyone other then an Event Hub expert.
  config :prefetch_count, :validate => :number, :default => 300

  # The max time allowed receive events without a timeout.
  # Value is expressed in seconds, default 60
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    receive_timeout => 60
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           receive_timeout => 300
  #       }}
  #    ]
  # }
  # NOTE - This option is intentionally not part of the public documentation. This is a very low level configuration that shouldn't need to be changed by anyone other then an Event Hub expert.
  config :receive_timeout, :validate => :number, :default => 60

  # When first reading from an event hub, start from this position.
  # beginning - reads ALL pre-existing events in the event hub
  # end - reads NO pre-existing events in the event hub
  # look_back - reads end minus N seconds worth of pre-existing events
  # Note - If the storage_connection is set, this configuration is only applicable for the very first time Logstash reads from the event hub.
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    initial_position => "beginning"
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           initial_position => "beginning"
  #       }}
  #    ]
  # }
  config :initial_position, :validate => ['beginning', 'end', 'look_back'], :default => 'beginning'

  # The number of seconds to look back for pre-existing events to determine the initial position.
  # Note - If the storage_connection is set, this configuration is only applicable for the very first time Logstash reads from the event hub.
  # Note - this options is only used when initial_position => "look_back"
  # Value is expressed in seconds, default is 1 day
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    initial_position => "look_back"
  #    initial_position_look_back => 86400
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           initial_position => "look_back"
  #           initial_position_look_back => 86400
  #       }}
  #    ]
  # }
  config :initial_position_look_back, :validate => :number, :default => 86400

  # The interval in seconds between writing checkpoint while processing a batch. Default 5 seconds. Checkpoints can slow down processing, but are needed to know where to start after a restart.
  # Note - checkpoints happen after every batch, so this configuration is only applicable while processing a single batch.
  # Value is expressed in seconds, set to zero to disable
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    checkpoint_interval => 5
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           checkpoint_interval => 5
  #       }}
  #    ]
  # }
  config :checkpoint_interval, :validate => :number, :default => 5

  # Adds meta data to the event.
  # [@metadata][azure_event_hubs][name] - the name of hte event host
  # [@metadata][azure_event_hubs][consumer_group] - the consumer group that consumed this event
  # [@metadata][azure_event_hubs][processor_host] - the unique identifier that identifies which host processed this event. Note - there can be multiple processor hosts on a single instance of Logstash.
  # [@metadata][azure_event_hubs][partition] - the partition from which event came from
  # [@metadata][azure_event_hubs][offset] - the event hub offset for this event
  # [@metadata][azure_event_hubs][sequence] - the event hub sequence for this event
  # [@metadata][azure_event_hubs][timestamp] - the enqueued time of the event
  # [@metadata][azure_event_hubs][event_size] - the size of the event
  # basic Example:
  # azure_event_hubs {
  #    config_mode => "basic"
  #    event_hub_connections => ["Endpoint=sb://example1...;EntityPath=event_hub_name1"]
  #    decorate_events => true
  # }
  # advanced example:
  # azure_event_hubs {
  #   config_mode => "advanced"
  #   event_hubs => [
  #       { "event_hub_name1" => {
  #           event_hub_connection => "Endpoint=sb://example1..."
  #           decorate_events => true
  #       }}
  #    ]
  # }
  config :decorate_events, :validate => :boolean, :default => false

  # Azure Active Directory (AAD) Authentication Configuration
  # When using AAD authentication, provide: aad_tenant_id, aad_client_id, aad_client_secret, and event_hub_namespace
  # With AAD auth, event_hub_connections should contain just the Event Hub names (e.g., ["my-event-hub"])
  # Note: AAD mode uses in-memory checkpointing and processes all partitions

  # The Azure Active Directory Tenant ID (Directory ID)
  # Found in Azure Portal > Azure Active Directory > Properties > Tenant ID
  config :aad_tenant_id, :validate => :string, :required => false

  # The Azure Active Directory Application (Client) ID
  # The app must have "Azure Event Hubs Data Receiver" role on the Event Hub namespace
  config :aad_client_id, :validate => :string, :required => false

  # The Azure Active Directory Client Secret
  config :aad_client_secret, :validate => :password, :required => false

  # The Event Hub fully qualified namespace (e.g., "my-namespace.servicebus.windows.net")
  # Required when using AAD authentication
  config :event_hub_namespace, :validate => :string, :required => false

  attr_reader :count

  def initialize(params)

    # explode the all of the parameters to be scoped per event_hub
    @event_hubs_exploded = []
    # global_config will be merged into the each of the exploded configs, prefer any configuration already scoped over the globally scoped config
    global_config = {}
    params.each do |k, v|
      if !k.eql?('id') && !k.eql?('event_hubs') && !k.eql?('threads') && !k.eql?('event_hub_connections')  # don't copy these to the per-event-hub configs
        global_config[k] = v
      end
    end

    # Check if AAD authentication is being used
    @use_aad_auth = !!(params['aad_tenant_id'] && params['aad_client_id'] && params['aad_client_secret'] && params['event_hub_namespace'])
    
    if @use_aad_auth
      # Validate AAD config
      validate_aad_config(params)
      # Protect client secret from leaking in logs
      if params['aad_client_secret'].is_a?(String)
        params['aad_client_secret'] = ::LogStash::Util::Password.new(params['aad_client_secret'])
      end
    end

    if params['config_mode'] && params['config_mode'].eql?('advanced')
      params['event_hub_connections'] = ['dummy'] # trick the :required validation

      params['event_hubs'].each do |event_hub|
        raise "event_hubs must be a Hash" unless event_hub.is_a?(Hash)
        event_hub.each do |event_hub_name, config|
          config.each do |k, v|
            if 'event_hub_connection'.eql?(k) || 'storage_connection'.eql?(k) # protect from leaking logs
              config[k] = ::LogStash::Util::Password.new(v)
            end
          end
          if config['event_hub_connection'] #add the 's' to pass validation
            config['event_hub_connections'] = config['event_hub_connection']
            config.delete('event_hub_connection')
          end

          config.merge!({'event_hubs' => [event_hub_name]})
          config.merge!(global_config) {|k, v1, v2| v1}
          @event_hubs_exploded << config
        end
      end
    else # basic config
      params['event_hubs'] = ['dummy'] # trick the :required validation
      if params['event_hub_connections']
        connections = *params['event_hub_connections'] # ensure array
        connections.each.with_index do |_connection, i|
          begin
            connection = replace_connection_placeholders(_connection)
            if @use_aad_auth
              # For AAD auth, connection is just the event hub name
              event_hub_name = connection.strip
              params['event_hub_connections'][i] = event_hub_name
            else
              event_hub_name = ConnectionStringBuilder.new(connection).getEventHubName
              redacted_connection = connection.gsub(/(SharedAccessKey=)([0-9a-zA-Z=+]*)([;]*)(.*)/, '\\1<redacted>\\3\\4')
              params['event_hub_connections'][i] = redacted_connection # protect from leaking logs
            end
            raise "invalid Event Hub name" unless event_hub_name && !event_hub_name.empty?
          rescue => e
            if @use_aad_auth
              raise LogStash::ConfigurationError, "Error: invalid Event Hub name '#{connection}'"
            else
              raise LogStash::ConfigurationError, "Error parsing event hub string name for connection: '#{redacted_connection rescue connection}' please ensure that the connection string contains the EntityPath"
            end
          end
          exploded = {'event_hubs' => [event_hub_name]}.merge(global_config) {|k, v1, v2| v1}
          # Always include event_hub_connections for validation (use event hub name as placeholder for AAD mode)
          exploded['event_hub_connections'] = [::LogStash::Util::Password.new(@use_aad_auth ? event_hub_name : connection)]
          @event_hubs_exploded << exploded
        end
      end
    end

    super(params)

    container_consumer_groups = []
    # explicitly validate all the per event hub configs
    @event_hubs_exploded.each do |event_hub|
      if !self.class.validate(event_hub)
        raise LogStash::ConfigurationError, I18n.t("logstash.runner.configuration.invalid_plugin_settings")
      end
      container_consumer_groups << {event_hub['storage_connection'].value.to_s + (event_hub['storage_container'] ? event_hub['storage_container'] : event_hub['event_hubs'][0]) => event_hub['consumer_group']} if event_hub['storage_connection']
    end
    raise "The configuration will result in overwriting offsets. Please ensure that the each Event Hub's consumer_group is using a unique storage container." if container_consumer_groups.size > container_consumer_groups.uniq.size
  end

  attr_reader :event_hubs_exploded

  def replace_connection_placeholders(value)
    connection = value
    if self.class.respond_to? :replace_placeholders
      # Logstash 6.x
      if self.class.method(:replace_placeholders).arity == 1
        connection = self.class.replace_placeholders(connection)
      else
        # Logstash 8.15.1 and 8.15.2 changed the method arity including a new boolean parameter `refine`.
        # This was fixed in 8.15.3. see https://github.com/elastic/logstash/pull/16485
        connection = self.class.replace_placeholders(connection, false)
      end
    end

    # Logstash 5.x
    connection = self.class.replace_env_placeholders(connection) if self.class.respond_to? :replace_env_placeholders

    connection
  end

  def register
    # augment the exploded config with the defaults
    @event_hubs_exploded.each do |event_hub|
      @config.each do |key, value|
        if !key.eql?('id') && !key.eql?('event_hubs')
          event_hub[key] = value unless event_hub[key]
        end
      end
    end
    @logger.debug("Exploded Event Hub configuration.",  :event_hubs_exploded => @event_hubs_exploded.to_s)
  end

  def run(queue)
    if @use_aad_auth
      run_with_aad_auth(queue)
    else
      run_with_connection_string(queue)
    end
  end

  # Run using AAD authentication with EventHubClient directly
  def run_with_aad_auth(queue)
    event_hub_threads = []
    executor_service = Executors.newScheduledThreadPool(@threads)
    @aad_clients = []
    @aad_receivers = []

    @event_hubs_exploded.each do |event_hub|
      event_hub_threads << Thread.new do
        event_hub_name = event_hub['event_hubs'].first
        @logger.info("Event Hub #{event_hub_name} is initializing with AAD authentication...")
        
        begin
          # Create EventHubClient with AAD authentication
          client = create_aad_event_hub_client(event_hub_name, executor_service)
          @aad_clients << client
          
          # Get runtime info to discover partitions
          runtime_info = client.getRuntimeInformation.get
          partition_ids = runtime_info.getPartitionIds
          
          @logger.info("Event Hub #{event_hub_name} has #{partition_ids.length} partitions: #{partition_ids.to_a.join(', ')}")
          
          # Determine initial position
          initial_position = get_initial_event_position(event_hub)
          
          # Create receivers for all partitions
          partition_ids.each do |partition_id|
            receiver = client.createReceiverSync(
              event_hub['consumer_group'],
              partition_id,
              initial_position
            )
            receiver.setPrefetchCount(event_hub['prefetch_count'])
            @aad_receivers << receiver
            
            @logger.info("Created receiver for partition #{partition_id} on #{event_hub_name}")
            
            # Start a thread to receive events from this partition
            Thread.new do
              process_partition_events(receiver, partition_id, event_hub_name, event_hub, queue)
            end
          end
          
          @logger.info("Event Hub #{event_hub_name} is processing events with AAD auth...")
          
          # Keep running until stop is requested
          while !stop?
            Stud.stoppable_sleep(1) { stop? }
          end
          
        rescue => e
          @logger.error("Event Hub failed during AAD initialization.", :event_hub_name => event_hub_name, :exception => e, :backtrace => e.backtrace)
          do_stop
        end
      end
    end

    # Wait until stop is requested
    while !stop?
      Stud.stoppable_sleep(1) { stop? }
    end

    # Cleanup
    @logger.info("Shutting down AAD receivers...")
    @aad_receivers.each do |receiver|
      begin
        receiver.closeSync
      rescue => e
        @logger.debug("Error closing receiver", :exception => e)
      end
    end

    @logger.info("Shutting down AAD clients...")
    @aad_clients.each do |client|
      begin
        client.closeSync
      rescue => e
        @logger.debug("Error closing client", :exception => e)
      end
    end

    event_hub_threads.each(&:join)
    executor_service.shutdown
    executor_service.awaitTermination(10, TimeUnit::MINUTES) rescue nil
  end

  # Run using connection string with EventProcessorHost (original behavior)
  def run_with_connection_string(queue)
    event_hub_threads = []
    named_thread_factory = LogStash::Inputs::Azure::NamedThreadFactory.new("azure_event_hubs-worker", @id)
    scheduled_executor_service = Executors.newScheduledThreadPool(@threads, named_thread_factory)
    @event_hubs_exploded.each do |event_hub|
      event_hub_threads << Thread.new do
        event_hub_name = event_hub['event_hubs'].first # there will always only be 1 from @event_hubs_exploded
        @logger.info("Event Hub #{event_hub_name} is initializing... ")
        begin
          if event_hub['storage_connection']
            event_processor_host = EventProcessorHost::EventProcessorHostBuilder.newBuilder(EventProcessorHost.createHostName('logstash'), event_hub['consumer_group'])
                                                         .useAzureStorageCheckpointLeaseManager(
                                                           event_hub['storage_connection'].value,
                                                           event_hub.fetch('storage_container', event_hub_name),
                                                           nil
                                                         )
                                                         .useEventHubConnectionString(
                                                           event_hub['event_hub_connections'].first.value, #there will only be one in this array by the time it gets here
                                                         )
                                                     .setExecutor(scheduled_executor_service)
                                                     .build
          else
            @logger.warn("You have NOT specified a `storage_connection_string` for #{event_hub_name}. This configuration is only supported for a single Logstash instance.")
            event_processor_host = create_in_memory_event_processor_host(event_hub, event_hub_name, scheduled_executor_service)
          end
          options = EventProcessorOptions.new
          options.setMaxBatchSize(event_hub['max_batch_size'])
          options.setPrefetchCount(event_hub['prefetch_count'])
          options.setReceiveTimeOut(Duration.ofSeconds(event_hub['receive_timeout']))
          
          options.setExceptionNotification(LogStash::Inputs::Azure::ErrorNotificationHandler.new)
          case event_hub['initial_position']
          when 'beginning'
            msg = "Configuring Event Hub #{event_hub_name} to read events all events."
            @logger.debug("If this is the initial read... " + msg) if event_hub['storage_connection']
            @logger.info(msg) unless event_hub['storage_connection']
            options.setInitialPositionProvider(EventProcessorOptions::StartOfStreamInitialPositionProvider.new(options))
          when 'end'
            msg = "Configuring Event Hub #{event_hub_name} to read only new events."
            @logger.debug("If this is the initial read... " + msg) if event_hub['storage_connection']
            @logger.info(msg) unless event_hub['storage_connection']
            options.setInitialPositionProvider(EventProcessorOptions::EndOfStreamInitialPositionProvider.new(options))
          when 'look_back'
            msg = "Configuring Event Hub #{event_hub_name} to read events starting at 'now - #{event_hub['initial_position_look_back']}' seconds."
            @logger.debug("If this is the initial read... " + msg) if event_hub['storage_connection']
            @logger.info(msg) unless event_hub['storage_connection']
            options.setInitialPositionProvider(LogStash::Inputs::Azure::LookBackPositionProvider.new(event_hub['initial_position_look_back']))
          end
          event_processor_host.registerEventProcessorFactory(LogStash::Inputs::Azure::ProcessorFactory.new(queue, event_hub['codec'], event_hub['checkpoint_interval'], self.method(:decorate), event_hub['decorate_events']), options)
              .when_complete(lambda {|x, e|
                @logger.info("Event Hub registration complete. ", :event_hub_name => event_hub_name )
                @logger.error("Event Hub failure while registering.", :event_hub_name => event_hub_name, :exception => e, :backtrace => e.backtrace) if e
              })
              .then_accept(lambda {|x|
                @logger.info("Event Hub is processing events... ", :event_hub_name => event_hub_name )
                # this blocks the completable future chain from progressing, actual work is done via the executor service
                while !stop?
                  Stud.stoppable_sleep(1) {stop?}
                end
              })
              .then_compose(lambda {|x|
                @logger.info("Unregistering Event Hub this can take a while... ", :event_hub_name => event_hub_name )
                event_processor_host.unregisterEventProcessor
              })
              .exceptionally(lambda {|e|
                @logger.error("Event Hub encountered an error.", :event_hub_name => event_hub_name , :exception => e, :backtrace => e.backtrace) if e
                nil
              })
              .get # this blocks till all of the futures are complete.
          @logger.info("Event Hub #{event_hub_name} is closed.")
        rescue => e
          @logger.error("Event Hub failed during initialization.", :event_hub_name => event_hub_name, :exception => e, :backtrace => e.backtrace) if e
          do_stop
        end
      end
    end

    # this blocks the input from existing. (all work is being done in threads)
    while !stop?
      Stud.stoppable_sleep(1) {stop?}
    end

    # This blocks the input till all the threads have run to completion.
    event_hub_threads.each do |thread|
      thread.join
    end

    # Ensure proper shutdown of executor service. # Note - this causes a harmless warning in the logs that scheduled tasks are being rejected.
    scheduled_executor_service.shutdown
    begin
      scheduled_executor_service.awaitTermination(10, TimeUnit::MINUTES);
    rescue => e
      @logger.debug("interrupted while waiting to close executor service, this can generally be ignored", :exception => e, :backtrace => e.backtrace) if e
    end
  end

  def create_in_memory_event_processor_host(event_hub, event_hub_name, scheduled_executor_service)
    checkpoint_manager = InMemoryCheckpointManager.new
    lease_manager = InMemoryLeaseManager.new
    event_processor_host = EventProcessorHost::EventProcessorHostBuilder.newBuilder(EventProcessorHost.createHostName('logstash'), event_hub['consumer_group'])
                                             .useUserCheckpointAndLeaseManagers(checkpoint_manager, lease_manager)
                                             .useEventHubConnectionString(event_hub['event_hub_connections'].first.value) #there will only be one in this array by the time it gets here
                                             .setExecutor(scheduled_executor_service)
                                             .build
    host_context = get_host_context(event_processor_host)
    #using java_send to avoid naming conflicts with 'initialize' method
    lease_manager.java_send :initialize, [HostContext], host_context
    checkpoint_manager.java_send :initialize, [HostContext], host_context
    event_processor_host
  end

  private

  # This method is used to get around the fact that recent versions of jruby do not
  # allow access to the package private protected method `getHostContext`
  def get_host_context(event_processor_host)
    call_private(event_processor_host, 'getHostContext')
  end

  def call_private(clazz, method)
    method = clazz.java_class.declared_method(method)
    method.accessible = true
    method.invoke(clazz)
  end

  # Validate AAD configuration
  def validate_aad_config(params)
    missing = []
    missing << 'aad_tenant_id' unless params['aad_tenant_id']
    missing << 'aad_client_id' unless params['aad_client_id']
    missing << 'aad_client_secret' unless params['aad_client_secret']
    missing << 'event_hub_namespace' unless params['event_hub_namespace']

    unless missing.empty?
      raise LogStash::ConfigurationError, "AAD authentication requires: #{missing.join(', ')}"
    end
  end

  # Create EventHubClient with AAD authentication
  def create_aad_event_hub_client(event_hub_name, executor_service)
    namespace = @event_hub_namespace
    # Ensure namespace has the full FQDN
    namespace = "#{namespace}.servicebus.windows.net" unless namespace.include?('.')
    
    endpoint = URI.new("sb://#{namespace}/")
    authority = "https://login.microsoftonline.com/#{@aad_tenant_id}/"
    
    @logger.debug("Creating AAD EventHubClient", 
                  :namespace => namespace, 
                  :event_hub => event_hub_name,
                  :authority => authority)

    # Create the authentication callback
    auth_callback = create_aad_auth_callback

    # Create EventHubClient with AAD authentication
    client_future = EventHubClient.createWithAzureActiveDirectory(
      endpoint,
      event_hub_name,
      auth_callback,
      authority,
      executor_service,
      nil  # EventHubClientOptions - use defaults
    )

    client_future.get
  end

  # Create AAD authentication callback using ADAL4J
  def create_aad_auth_callback
    tenant_id = @aad_tenant_id
    client_id = @aad_client_id
    client_secret = @aad_client_secret.value
    logger = @logger

    # Create a Java proxy for the AuthenticationCallback interface
    callback = java.lang.reflect.Proxy.newProxyInstance(
      AzureActiveDirectoryTokenProvider::AuthenticationCallback.java_class.class_loader,
      [AzureActiveDirectoryTokenProvider::AuthenticationCallback.java_class].to_java(java.lang.Class),
      AadAuthCallbackHandler.new(tenant_id, client_id, client_secret, logger)
    )

    callback
  end

  # Get EventPosition based on initial_position config
  def get_initial_event_position(event_hub)
    case event_hub['initial_position']
    when 'beginning'
      EventPosition.fromStartOfStream
    when 'end'
      EventPosition.fromEndOfStream
    when 'look_back'
      look_back_seconds = event_hub['initial_position_look_back'] || 86400
      instant = java.time.Instant.now.minusSeconds(look_back_seconds)
      EventPosition.fromEnqueuedTime(instant)
    else
      EventPosition.fromStartOfStream
    end
  end

  # Process events from a partition
  def process_partition_events(receiver, partition_id, event_hub_name, event_hub, queue)
    codec = event_hub['codec'].clone
    decorate_events = event_hub['decorate_events']
    consumer_group = event_hub['consumer_group']
    max_batch_size = event_hub['max_batch_size']
    receive_timeout = Duration.ofSeconds(event_hub['receive_timeout'])

    while !stop?
      begin
        # Receive events
        events = receiver.receive(max_batch_size, receive_timeout).get
        
        next if events.nil?

        events.each do |event_data|
          begin
            body = String.from_java_bytes(event_data.getBytes)
            
            codec.decode(body) do |decoded_event|
              if decorate_events
                decoded_event.set("[@metadata][azure_event_hubs][name]", event_hub_name)
                decoded_event.set("[@metadata][azure_event_hubs][consumer_group]", consumer_group)
                decoded_event.set("[@metadata][azure_event_hubs][partition]", partition_id)
                decoded_event.set("[@metadata][azure_event_hubs][offset]", event_data.getSystemProperties.getOffset)
                decoded_event.set("[@metadata][azure_event_hubs][sequence]", event_data.getSystemProperties.getSequenceNumber)
                decoded_event.set("[@metadata][azure_event_hubs][timestamp]", event_data.getSystemProperties.getEnqueuedTime.toEpochMilli)
                decoded_event.set("[@metadata][azure_event_hubs][event_size]", event_data.getBytes.length)
                
                # Add user properties if present
                props = event_data.getProperties
                if props && !props.isEmpty
                  props.each do |key, value|
                    decoded_event.set("[@metadata][azure_event_hubs][user_properties][#{key}]", value.to_s)
                  end
                end
              end
              
              decorate(decoded_event)
              queue << decoded_event
            end
          rescue => e
            @logger.error("Error processing event", :partition => partition_id, :exception => e, :backtrace => e.backtrace)
          end
        end
      rescue java.util.concurrent.TimeoutException
        # Timeout is expected when no events, just continue
      rescue => e
        if !stop?
          @logger.error("Error receiving events from partition", :partition => partition_id, :exception => e, :backtrace => e.backtrace)
          Stud.stoppable_sleep(5) { stop? }  # Back off before retry
        end
      end
    end
  end

  # Inner class to handle AAD authentication callback
  class AadAuthCallbackHandler
    include java.lang.reflect.InvocationHandler

    def initialize(tenant_id, client_id, client_secret, logger)
      @tenant_id = tenant_id
      @client_id = client_id
      @client_secret = client_secret
      @logger = logger
      @executor_service = java.util.concurrent.Executors.newCachedThreadPool
    end

    def invoke(proxy, method, args)
      method_name = method.getName
      
      if method_name == "acquireToken"
        audience = args[0]
        authority = args[1]
        state = args[2]
        acquire_token(audience, authority, state)
      elsif method_name == "toString"
        "AadAuthCallbackHandler"
      elsif method_name == "hashCode"
        self.hash
      elsif method_name == "equals"
        self == args[0]
      else
        nil
      end
    end

    def acquire_token(audience, authority, state)
      @logger.debug("Acquiring AAD token", :audience => audience, :authority => authority)

      begin
        # Build authority URL
        auth_url = authority.end_with?('/') ? authority : "#{authority}/"
        
        auth_context = AuthenticationContext.new(auth_url, false, @executor_service)
        credential = ClientCredential.new(@client_id, @client_secret)

        # Acquire token - audience for Event Hubs is typically "https://eventhubs.azure.net/"
        future = auth_context.acquireToken(audience, credential, nil)
        result = future.get

        if result.nil?
          raise "Failed to acquire AAD token - result is null"
        end

        @logger.debug("AAD token acquired successfully", :expires_on => result.getExpiresOnDate.to_s)

        # Return completed future with the access token
        CompletableFuture.completedFuture(result.getAccessToken)
      rescue => e
        @logger.error("Failed to acquire AAD token", :exception => e.message)
        failed_future = CompletableFuture.new
        failed_future.completeExceptionally(java.lang.Exception.new(e.message))
        failed_future
      end
    end
  end

end
