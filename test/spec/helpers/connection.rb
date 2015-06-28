require 'yaml'
require 'json'
require 'securerandom'
require 'fiber'
require '../lib/gilmour/protocol'
require 'em-hiredis'

require_relative 'common'

def options(which)
  data = YAML::load(File.open("#{File.dirname(__FILE__)}/data.yml"))
  data[which]
end

def redis_connection_options
  options(:connection)
end

def redis_ping_options
  options(:ping)
end

def redis_wildcard_options
  options(:wildcard)
end

def redis_publish_async(options, message, key)
  EM.defer do
    redis = EM::Hiredis.connect
    payload, _ = Gilmour::Protocol.create_request(message)
    redis.publish(key, payload)
  end
end

def redis_send_and_recv(options, message, key)
  waiter = Waiter.new
  response = code = nil
  operation = proc do
    redis = EM::Hiredis.connect
    payload, sender = Gilmour::Protocol.create_request(message)
    response_topic = "gilmour.response.#{sender}"
    redis.pubsub.subscribe(response_topic)
    redis.pubsub.on(:message) do |topic, data|
      begin
        response, code, _ = Gilmour::Protocol.parse_response(data)
        waiter.signal
      rescue Exception => e
        $stderr.puts e.message
      end
    end
    redis.publish(key, payload)
  end
  EM.defer(operation)
  waiter.wait
  [response, code]
end

