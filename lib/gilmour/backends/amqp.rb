require 'amqp'
require_relative 'backend'

module Gilmour
  class AmqpBackend < Backend
    implements 'amqp'
    attr_reader :connection
    attr_reader :channel
    attr_reader :exchange

    def initialize(options)
      waiter = Thread.new { loop { sleep 1 } }
      Thread.new do
        AMQP.start(host: options[:host]) do |connection|
          @connection = connection
          initialize_amqp_channel(options) do
            waiter.kill
          end
        end
      end
      waiter.join
    end

    def setup_subscribers(subs)
      subs.each do |topic, subscribers|
        subscribers.each do |subscriber|
          setup_subscriber(topic, subscriber[:handler])
        end
      end
    end


    private

    def initialize_amqp_channel(options)
      AMQP::Channel.new(@connection) do |channel|
        @channel = channel
        initialize_amqp_exchange(options)
        yield if block_given?
      end
    end

    def initialize_amqp_exchange(options)
      @exchange = channel.topic(options[:exchange])
    end

    def queue_name(subscriber, topic)
      "#{subscriber}_#{topic}_queue"
    end

    def setup_subscriber(topic, handler)
      @channel.queue(queue_name(self.class, topic))
      .bind(@exchange, routing_key: topic)
      .subscribe do |headers, payload|
        execute_handler(headers.routing_key, payload, handler)
        .execute(handler)
        send_async(body, sender, code) if code && sender
      end
    end

    def send(data, destination, code = nil)
      payload, _ = Gilmour::Protocol.create_request(data, code)
      @exchange.publish(payload, routing_key: destination)
    end
  end
end
