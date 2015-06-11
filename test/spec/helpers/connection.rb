require 'yaml'
require 'json'
require 'securerandom'
require 'fiber'
require '../lib/gilmour/protocol'
require 'em-hiredis'

def amqp_options(which)
  amqp = YAML::load(File.open("#{File.dirname(__FILE__)}/amqp.yml"))
  amqp[which]
end

def amqp_connection_options
  amqp_options(:connection)
end

def amqp_ping_options
  amqp_options(:ping)
end

def amqp_wildcard_options
  amqp_options(:wildcard)
end

def redis_connection_options
  amqp_options(:connection)
end

def redis_ping_options
  amqp_options(:ping)
end

def redis_wildcard_options
  amqp_options(:wildcard)
end

def amqp_publish_async(options, message, key)
  operation = proc do
    AMQP.connect(host: options[:host]) do |connection|
      AMQP::Channel.new(connection) do |channel|
        exchange = channel.topic(options[:exchange])
        payload, _ = Gilmour::Protocol.create_request(message)
        exchange.publish(payload, routing_key: key)
      end
    end
  end
  EM.defer(operation)
end

def redis_publish_async(options, message, key)
  operation = proc do
    redis = EM::Hiredis.connect
    payload, _ = Gilmour::Protocol.create_request(message)
    redis.publish(key, payload)
  end
  EM.defer(operation)
end

def amqp_send_and_recv(options, message, key)
  waiter = Thread.new { loop { sleep 1 } }
  response = code = nil
  operation = proc do
    AMQP.connect(host: options[:host]) do |connection|
      AMQP::Channel.new(connection) do |channel|
        exchange = channel.topic(options[:exchange])
        payload, sender = Gilmour::Protocol.create_request(message)
        response_topic = "response.#{sender}"
        channel.queue(response_topic).bind(exchange, routing_key: response_topic).subscribe do |headers, data|
          begin
            response, code, _ = Gilmour::Protocol.parse_response(data)
            waiter.kill
          rescue Exception => e
            $stderr.puts e.message
          end
        end
        exchange.publish(payload, routing_key: key)
      end
    end
  end
  EM.defer(operation)
  waiter.join
  [response, code]
end

def redis_send_and_recv(options, message, key)
  waiter = Thread.new { loop { sleep 1 } }
  response = code = nil
  operation = proc do
    redis = EM::Hiredis.connect
    payload, sender = Gilmour::Protocol.create_request(message)
    response_topic = "gilmour.response.#{sender}"
    redis.pubsub.subscribe(response_topic)
    redis.pubsub.on(:message) do |topic, data|
      begin
        response, code, _ = Gilmour::Protocol.parse_response(data)
        waiter.kill
      rescue Exception => e
        $stderr.puts e.message
      end
    end
    redis.publish(key, payload)
  end
  EM.defer(operation)
  waiter.join
  [response, code]
end

