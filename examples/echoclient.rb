require 'gilmour/protocol'
require 'em-hiredis'

@count = 0
def subscribe_and_send_payload(redis, message, key)
  @count = (@count + 1) % 10
  key = "#{key}.#{SecureRandom.hex(2)}"
  m = @count == 0 ? 'Palmolive' : message
  payload, sender = Gilmour::Protocol.create_request(m)
  response_topic = "response.#{sender}"
  redis.pubsub.subscribe(response_topic)
  redis.publish(key, payload)
  puts "#{key}: #{payload}"
  response_topic
end

def redis_send_and_recv(message, key)
  EM.run do
    redis = EM::Hiredis.connect
    response_topic = subscribe_and_send_payload(redis, message, key)
    redis.pubsub.on(:message) do |topic, data|
      response, code, _ = Gilmour::Protocol.parse_response(data)
      puts response
      redis.pubsub.unsubscribe(response_topic)
      sleep 1
      response_topic = subscribe_and_send_payload(redis, message, key)
    end
  end
end

redis_send_and_recv('Ping', 'echo')

