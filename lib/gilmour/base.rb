# encoding: utf-8
require 'amqp'
require 'securerandom'
require 'json'
require 'mash'
require_relative 'protocol'
require_relative 'responder'

# The Gilmour module
module Gilmour
  RUNNING = false
  # This is the base module that should be included into the
  # container class
  module Base
    def self.included(base)
      base.extend(Registrar)
    end

    ######### Registration module ###########
    # This module helps act as a Resistrar for subclasses
    module Registrar
      attr_accessor subscribers_path
      @@subscribers = {} # rubocop:disable all
      @subscribers_path = 'subscribers'

      def listen_to(topic)
        handler = Proc.new
        @@subscribers[topic] ||= []
        @@subscribers[topic] << { handler: handler, subscriber: self }
      end

      def subscribers(topic = nil)
        if topic
          @@subscribers[topic]
        else
          @@subscribers
        end
      end

      def load_all(dir = nil)
        dir ||= subscribers_path
        Dir["#{dir}/*.rb"].each { |f| require f }
      end

      def load_subscriber(path)
        require path
      end
    end
    ############ End Register ###############

    attr_reader :connection
    attr_reader :channel
    attr_reader :exchange

    def initialize_amqp_connection(options)
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

    def start
      self.class.subscribers.each do |topic, subscribers|
        subscribers.each do |subscriber|
          setup_subscriber(topic,
                           subscriber[:subscriber],
                           subscriber[:handler])
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

    def setup_subscriber(topic, sub, handler)
      @channel.queue(queue_name(sub, topic))
      .bind(@exchange, routing_key: topic)
      .subscribe do |headers, payload|
        data, sender = Gilmour::Protocol.parse_request(payload)
        body, code = Gilmour::Responder.new(headers.routing_key, data)
          .execute(handler)
        send_async(body, code, sender) if code && sender
      end
    end

    def send_async(data, code, destination)
      payload, _ = Gilmour::Protocol.create_request(data, code)
      key = "response.#{destination}"
      @exchange.publish(payload, routing_key: key)
    end
  end
end
