# encoding: utf-8
require "logstash/util/loggable"

module LogStash
  module Inputs
    module Azure
      class ErrorNotificationHandler
        include LogStash::Util::Loggable

        java_import com.azure.messaging.eventhubs.models.ErrorContext

        def initialize
          @logger = self.logger
        end

        # Called by EventProcessorClient when an error occurs
        def handle_error(error_context)
          partition_context = error_context.getPartitionContext
          throwable = error_context.getThrowable

          @logger.error("Error with Event Processor.",
            :event_hub_name => partition_context.getEventHubName,
            :consumer_group => partition_context.getConsumerGroup,
            :partition_id => partition_context.getPartitionId,
            :fully_qualified_namespace => partition_context.getFullyQualifiedNamespace,
            :exception => throwable.nil? ? "unknown" : throwable.getMessage,
            :exception_class => throwable.nil? ? "unknown" : throwable.getClass.getName)
        end
      end
    end
  end
end
