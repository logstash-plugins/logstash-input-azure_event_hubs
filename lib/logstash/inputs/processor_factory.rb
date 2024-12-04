# encoding: utf-8
require "logstash/inputs/processor"

module LogStash
  module Inputs
    module Azure
      class ProcessorFactory
        include com.microsoft.azure.eventprocessorhost.IEventProcessorFactory

        def initialize(queue, codec, checkpoint_interval, decorator, meta_data, checkpoint_each_batch)
          @queue = queue
          @codec = codec
          @checkpoint_interval = checkpoint_interval
          @decorator = decorator
          @meta_data = meta_data
          @checkpoint_each_batch = checkpoint_each_batch
        end

        def createEventProcessor(context)
          Processor.new(@queue, @codec.clone, @checkpoint_interval, @decorator, @meta_data, @checkpoint_each_batch)
        end

      end
    end
  end
end
