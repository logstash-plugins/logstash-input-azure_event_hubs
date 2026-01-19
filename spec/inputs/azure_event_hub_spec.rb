# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/azure_event_hubs"

java_import com.azure.messaging.eventhubs.EventProcessorClient
java_import com.azure.messaging.eventhubs.EventProcessorClientBuilder

describe LogStash::Inputs::AzureEventHubs do


  subject(:input) {LogStash::Plugin.lookup("input", "azure_event_hubs").new(config)}

  describe "Event Hubs Configuration -> " do
    shared_examples "an exploded Event Hub config" do |x|
      it "it explodes #{x} event hub(s) correctly" do
        exploded_config = input.event_hubs_exploded
        x.times do |i|
          expect(exploded_config[i]['event_hubs'].size).to be == 1 #always 1 in the exploded form
          expect(exploded_config[i]['event_hubs'][0]).to eql('event_hub_name' + i.to_s)
          expect(exploded_config[i]['event_hub_connections'][0].value).to start_with('Endpoint=sb://...')
          expect(exploded_config[i]['storage_connection'].value).to eql('DefaultEndpointsProtocol=https;AccountName=...')
          expect(exploded_config[i]['threads']).to be == 9
          expect(exploded_config[i]['codec'].class.to_s).to eq("LogStash::Codecs::Plain")
          expect(exploded_config[i]['consumer_group']).to eql('cg')
          expect(exploded_config[i]['max_batch_size']).to be == 20
          expect(exploded_config[i]['prefetch_count']).to be == 30
          expect(exploded_config[i]['receive_timeout']).to be == 40
          expect(exploded_config[i]['initial_position']).to eql('look_back')
          expect(exploded_config[i]['initial_position_look_back']).to be == 50
          expect(exploded_config[i]['checkpoint_interval']).to be == 60
          expect(exploded_config[i]['decorate_events']).to be_truthy
        end
      end
    end

    describe "Basic Config" do
      before do
        input.register
      end
      let(:config) do
        {
            'event_hub_connections' => ['Endpoint=sb://...;EntityPath=event_hub_name0', 'Endpoint=sb://...;EntityPath=event_hub_name1'],
            'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
            'threads' => 9,
            'codec' => 'plain',
            'consumer_group' => 'cg',
            'max_batch_size' => 20,
            'prefetch_count' => 30,
            'receive_timeout' => 40,
            'initial_position' => 'look_back',
            'initial_position_look_back' => 50,
            'checkpoint_interval' => 60,
            'decorate_events' => true
        }
      end
      it_behaves_like "an exploded Event Hub config", 2

      it "it runs the Event Processor Client" do
        mock_queue = double("queue")
        mock_client = double("event_processor_client")

        # rspec has issues with counters and concurrent code, so use threadsafe counters instead
        start_counter = java.util.concurrent.atomic.AtomicInteger.new
        stop_counter = java.util.concurrent.atomic.AtomicInteger.new
        builder_counter = java.util.concurrent.atomic.AtomicInteger.new

        expect(mock_client).to receive(:start).at_most(2).times {
          start_counter.incrementAndGet
        }
        expect(mock_client).to receive(:stop).at_most(2).times {
          stop_counter.incrementAndGet
        }

        allow(input).to receive(:build_event_processor_client).at_most(2).times { |event_hub, event_hub_name, processor|
          builder_counter.incrementAndGet
          mock_client
        }

        # signal the stop first since the run method blocks until stop is called.
        input.do_stop
        input.run(mock_queue)
        expect(builder_counter.get).to be == 2
        expect(start_counter.get).to be == 2
        expect(stop_counter.get).to be == 2
      end

      describe "single connection, no array syntax" do
        let(:config) do
          {
              'event_hub_connections' => 'Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=something;EntityPath=event_hub1'
          }
        end
        it "it can handle a single connection without the array notation" do
          expect {input}.to_not raise_error
          exploded_config = input.event_hubs_exploded
          expect(exploded_config.size).to be == 1
          expect(exploded_config[0]['event_hub_connections'][0].value).to eql('Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=something;EntityPath=event_hub1')
        end
      end
    end

    describe "Advanced Config" do
      before do
        input.register
      end
      let(:config) do
        {
            'config_mode' => 'advanced',
            'event_hubs' => [
                {'event_hub_name0' => {
                    'event_hub_connection' => 'Endpoint=sb://...',
                    'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
                    'codec' => 'plain',
                    'consumer_group' => 'cg',
                    'max_batch_size' => 20,
                    'prefetch_count' => 30,
                    'receive_timeout' => 40,
                    'initial_position' => 'look_back',
                    'initial_position_look_back' => 50,
                    'checkpoint_interval' => 60,
                    'decorate_events' => true}},
                {'event_hub_name1' => {
                    'event_hub_connection' => '1Endpoint=sb://...',
                    'storage_connection' => '1DefaultEndpointsProtocol=https;AccountName=...',
                    'codec' => 'json',
                    'consumer_group' => 'cg1',
                    'receive_timeout' => 41,
                    'initial_position' => 'end',
                    'checkpoint_interval' => 61,
                    'decorate_events' => false,
                    'storage_container' => 'alt_container'}},
                # same named event hub with different configuration is allowed
                {'event_hub_name0' => {
                    'event_hub_connection' => 'Endpoint=sb://...',
                    'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
                    'consumer_group' => 'ls'}}
            ],
            'codec' => 'plain',
            'consumer_group' => 'default_consumer_group',
            'max_batch_size' => 21,
            'prefetch_count' => 250,
            'receive_timeout' => 90,
            'initial_position' => 'beginning',
            'initial_position_look_back' => 7200,
            'checkpoint_interval' => 15,
            'decorate_events' => false,
            'threads' => 9
        }
      end
      it_behaves_like "an exploded Event Hub config", 1

      it "it explodes the second advanced config event hub correctly (with individual and inherited settings)" do
        exploded_config = input.event_hubs_exploded
        expect(exploded_config[1]['event_hubs'].size).to be == 1 #always 1 in the exploded form
        expect(exploded_config[1]['event_hubs'][0]).to eql('event_hub_name1')
        expect(exploded_config[1]['event_hub_connections'][0].value).to eql('1Endpoint=sb://...')
        expect(exploded_config[1]['storage_connection'].value).to eql('1DefaultEndpointsProtocol=https;AccountName=...')
        expect(exploded_config[1]['threads']).to be == 9
        expect(exploded_config[1]['codec'].class.to_s).to eq("LogStash::Codecs::JSON")
        expect(exploded_config[1]['consumer_group']).to eql('cg1')
        expect(exploded_config[1]['max_batch_size']).to be == 21
        expect(exploded_config[1]['prefetch_count']).to be == 250
        expect(exploded_config[1]['receive_timeout']).to be == 41
        expect(exploded_config[1]['initial_position']).to eql('end')
        expect(exploded_config[1]['initial_position_look_back']).to be == 7200
        expect(exploded_config[1]['checkpoint_interval']).to be == 61
        expect(exploded_config[1]['decorate_events']).to be_falsy
        expect(exploded_config[1]['storage_container']).to eq('alt_container')
      end

      it "it explodes the third advanced config event hub correctly (mostly inherited settings)" do
        exploded_config = input.event_hubs_exploded
        expect(exploded_config[2]['event_hubs'].size).to be == 1
        expect(exploded_config[2]['event_hubs'][0]).to eql('event_hub_name0')
        expect(exploded_config[2]['event_hub_connections'][0].value).to eql('Endpoint=sb://...')
        expect(exploded_config[2]['storage_connection'].value).to eql('DefaultEndpointsProtocol=https;AccountName=...')
        expect(exploded_config[2]['threads']).to be == 9
        expect(exploded_config[2]['codec'].class.to_s).to eq("LogStash::Codecs::Plain")
        expect(exploded_config[2]['consumer_group']).to eql('ls')
        expect(exploded_config[2]['max_batch_size']).to be == 21
        expect(exploded_config[2]['prefetch_count']).to be == 250
        expect(exploded_config[2]['receive_timeout']).to be == 90
        expect(exploded_config[2]['initial_position']).to eql('beginning')
        expect(exploded_config[2]['initial_position_look_back']).to be == 7200
        expect(exploded_config[2]['checkpoint_interval']).to be == 15
      end

      it "it runs the Event Processor Client" do
        mock_queue = double("queue")
        mock_client = double("event_processor_client")

        # rspec has issues with counters and concurrent code, so use threadsafe counters instead
        start_counter = java.util.concurrent.atomic.AtomicInteger.new
        stop_counter = java.util.concurrent.atomic.AtomicInteger.new
        builder_counter = java.util.concurrent.atomic.AtomicInteger.new

        expect(mock_client).to receive(:start).at_most(3).times {
          start_counter.incrementAndGet
        }
        expect(mock_client).to receive(:stop).at_most(3).times {
          stop_counter.incrementAndGet
        }

        allow(input).to receive(:build_event_processor_client).at_most(3).times { |event_hub, event_hub_name, processor|
          builder_counter.incrementAndGet
          mock_client
        }

        # signal the stop first since the run method blocks until stop is called.
        input.do_stop
        input.run(mock_queue)
        expect(builder_counter.get).to be == 3
        expect(start_counter.get).to be == 3
        expect(stop_counter.get).to be == 3
      end
    end

    describe "Bad Basic Config" do
      describe "Offset overwriting" do
        let(:config) do
          {
              'event_hub_connections' => ['Endpoint=sb://...;EntityPath=event_hub_name0', 'Endpoint=sb://...;EntityPath=event_hub_name0'],
              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/overwriting offsets/)
        end
      end

      describe "Invalid Event Hub name" do
        let(:config) do
          {
              'event_hub_connections' => ['Endpoint=sb://logstash/;SharedAccessKeyName=activity-log-readonly;SharedAccessKey=thisshouldnotbepartofthelogmessage'],
              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/that the connection string contains the EntityPath/)
          expect {input}.to raise_error(/redacted/)
          expect {input}.to raise_error(/^((?!thisshouldnotbepartofthelogmessage).)*$/)
        end
      end
    end

    describe "Bad Advanced Config" do
      describe "Offset overwritting" do
        let(:config) do
          {
              'config_mode' => 'advanced',
              'event_hubs' => [
                  {'event_hub_name0' => {
                      'event_hub_connection' => 'Endpoint=sb://...',
                  }},
                  {'event_hub_name1' => {
                      'event_hub_connection' => '1Endpoint=sb://...',
                  }}
              ],

              'storage_connection' => 'DefaultEndpointsProtocol=https;AccountName=...',
              'consumer_group' => 'default_consumer_group',
              'storage_container' => 'logstash'
          }
        end
        it "it errors when using same consumer group and storage container" do
          expect {input}.to raise_error(/overwriting offsets/)
        end
      end
    end
  end
end
