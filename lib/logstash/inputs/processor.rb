# encoding: utf-8
require "logstash/util/loggable"

module LogStash
  module Inputs
    module Azure
      class Processor
        include LogStash::Util::Loggable

        java_import com.azure.messaging.eventhubs.models.EventContext
        java_import com.azure.messaging.eventhubs.models.PartitionContext

        def initialize(queue, codec, checkpoint_interval, decorator, meta_data)
          @queue = queue
          @codec = codec
          @checkpoint_interval = checkpoint_interval
          @decorator = decorator
          @meta_data = meta_data
          @logger = self.logger
          @last_checkpoint_time = {}
        end

        # Called by EventProcessorClient for each event
        def process_event(event_context)
          partition_context = event_context.getPartitionContext
          event_data = event_context.getEventData

          return if event_data.nil?

          event_hub_name = partition_context.getEventHubName
          partition_id = partition_context.getPartitionId

          @logger.debug("Event Hub: #{event_hub_name}, Partition: #{partition_id} received event.") if @logger.debug?

          body_bytes = event_data.getBody
          byte_count = body_bytes.nil? ? 0 : body_bytes.length

          @logger.trace("Event Hub: #{event_hub_name}, Partition: #{partition_id}, Offset: #{event_data.getOffset},"+
                        " Sequence: #{event_data.getSequenceNumber}, Size: #{byte_count}") if @logger.trace?

          # Convert byte array to string
          body_string = body_bytes.nil? ? "" : String.from_java_bytes(body_bytes)

          @codec.decode(body_string) do |event|
            @decorator.call(event)

            if @meta_data
              event.set("[@metadata][azure_event_hubs][name]", event_hub_name)
              event.set("[@metadata][azure_event_hubs][consumer_group]", partition_context.getConsumerGroup)
              event.set("[@metadata][azure_event_hubs][processor_host]", partition_context.getFullyQualifiedNamespace)
              event.set("[@metadata][azure_event_hubs][partition]", partition_id)
              event.set("[@metadata][azure_event_hubs][offset]", event_data.getOffset)
              event.set("[@metadata][azure_event_hubs][sequence]", event_data.getSequenceNumber)
              enqueued_time = event_data.getEnqueuedTime
              event.set("[@metadata][azure_event_hubs][timestamp]", enqueued_time.nil? ? nil : enqueued_time.getEpochSecond)
              event.set("[@metadata][azure_event_hubs][event_size]", byte_count)
              # User properties
              properties = event_data.getProperties
              event.set("[@metadata][azure_event_hubs][user_properties]", properties.to_hash) if properties
            end

            @queue << event

            # Handle checkpointing
            if @checkpoint_interval > 0
              checkpoint_key = "#{event_hub_name}-#{partition_id}"
              last_checkpoint = @last_checkpoint_time[checkpoint_key] || 0
              now = Time.now.to_i
              since_last_checkpoint = now - last_checkpoint

              if since_last_checkpoint >= @checkpoint_interval
                begin
                  event_context.updateCheckpoint
                  @last_checkpoint_time[checkpoint_key] = now
                  @logger.debug("Checkpoint updated for partition #{partition_id}") if @logger.debug?
                rescue => e
                  @logger.warn("Failed to update checkpoint", :partition => partition_id, :error => e.message)
                end
              end
            end
          end

          @codec.flush
        end
      end
    end
  end
end
