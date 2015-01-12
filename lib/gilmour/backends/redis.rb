require 'em-hiredis'
require_relative 'backend'

module Gilmour
  # Redis backend implementation
  class RedisBackend < Backend
    implements 'redis'

    attr_reader :subscriber
    attr_reader :publisher

    def redis_host(opts)
      host = opts[:host] || '127.0.0.1'
      port = opts[:port] || 6379
      db = opts[:db] || 0
      "redis://#{host}:#{port}/#{db}"
    end

    def initialize(opts)
      @response_handlers = {}
      @subscriptions = {}
      waiter = Thread.new { loop { sleep 1 } }
      Thread.new do
        EM.run do
          setup_pubsub(opts)
          waiter.kill
        end
      end
      waiter.join
    end

    def setup_pubsub(opts)
      @publisher = EM::Hiredis.connect(redis_host(opts))
      @subscriber = @publisher.pubsub
      register_handlers
    end

    def register_handlers
      @subscriber.on(:pmessage) do |key, topic, payload|
        pmessage_handler(key, topic, payload)
      end
      @subscriber.on(:message) do |topic, payload|
        if topic.start_with? 'response.'
          response_handler(topic, payload)
        else
          pmessage_handler(topic, topic, payload)
        end
      end
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

    def register_response(sender, handler)
      topic = "response.#{sender}"
      @response_handlers[topic] = handler
      subscribe_topic(topic)
    end

    def response_handler(sender, payload)
      data, code, _ = Gilmour::Protocol.parse_response(payload)
      handler = @response_handlers[sender]
      if handler
        handler.call(data, code)
        @subscriber.unsubscribe(sender)
        @response_handlers.delete(sender)
      end
    end

    def setup_subscribers(subs = {})
      @subscriptions.merge!(subs)
      EM.defer do
        subs.keys.each { |topic| subscribe_topic(topic) }
      end
    end

    def add_listener(topic, &handler)
      @subscriptions[topic] ||= []
      @subscriptions[topic] << { handler: handler }
      subscribe_topic(topic)
    end

    def remove_listener(topic, handler = nil)
      if handler
        subs = @subscriptions[topic]
        subs.delete_if { |e| e[:handler] == handler }
      else
        @subscriptions[topic] = []
      end
      @subscriber.unsubscribe(topic) if @subscriptions[topic].empty?
    end

    def send(sender, destination, payload)
      register_response(sender, Proc.new) if block_given?
      @publisher.publish(destination, payload)
    end
  end
end
