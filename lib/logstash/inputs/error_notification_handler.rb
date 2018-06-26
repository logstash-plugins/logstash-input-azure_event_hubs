# encoding: utf-8
require "logstash/util/loggable"
java_import java.util.function.Consumer

module LogStash
  module Inputs
    module Azure
      class ErrorNotificationHandler
        include Consumer
        include LogStash::Util::Loggable

        def initialize
          @logger = self.logger
        end

        def accept(exception_received_event_args)
          @logger.error("Error with Event Processor Host. ", :exception_received_event_args => exception_received_event_args.to_s)
        end
      end
    end
  end
end
