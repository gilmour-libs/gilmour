require 'em-hiredis'
require_relative 'backend'

module Gilmour
  class RedisBackend < Backend
    implements 'redis'

    attr_reader :subscriber
    attr_reader :publisher

    def initialize(opts)
      @subscriptions = {}
      waiter = Thread.new { loop { sleep 1 } }
      Thread.new do
        EM.run do
          @publisher = EM::Hiredis.connect
          @subscriber = @publisher.pubsub
          waiter.kill
        end
      end
      waiter.join
    end

    def execute_handler(topic, payload, handler)
      data, sender = Gilmour::Protocol.parse_request(payload)
      body, code = Gilmour::Responder.new(topic, data)
      .execute(handler)
      send_async(body, code, sender) if code && sender
    end

    def subscribe_topic(topic)
      method = topic.index('*') ? :psubscribe : :subscribe
      @subscriber.method(method).call(topic)
    end

    def pmessage_handler(key, matched_topic, payload)
      @subscriptions[key].each do |subscription|
        execute_handler(matched_topic, payload, subscription[:handler])
      end
    end

    def setup_subscribers(subs)
      @subscriptions = subs
      EM.defer do
        subs.keys.each { |topic| subscribe_topic(topic) }
        @subscriber.on(:pmessage) do |key, topic, payload|
          pmessage_handler(key, topic, payload)
        end
        @subscriber.on(:message) do |topic, payload|
          pmessage_handler(topic, topic, payload)
        end
      end
    end

    def send_async(data, code, destination)
      payload, _ = Gilmour::Protocol.create_request(data, code)
      key = "response.#{destination}"
      @publisher.publish(key, payload)
    end
  end
end
