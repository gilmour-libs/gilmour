require 'gilmour/protocol'
require 'amqp'

def fib(initial, key)
  AMQP.start do |connection|
    AMQP::Channel.new(connection) do |channel|
      exchange = channel.topic('fib')
      payload, sender = Gilmour::Protocol.create_request(initial)
      response_topic = "response.#{sender}"
      channel.queue(response_topic).bind(exchange, routing_key: response_topic).subscribe do |headers, data|
        response, code, _ = Gilmour::Protocol.parse_response(data)
        puts response
        payload, _ = Gilmour::Protocol.create_request(response, nil, sender)
        sleep 1
        exchange.publish(payload, routing_key: "#{key}.next")
      end
      exchange.publish(payload, routing_key: "#{key}.init")
      sleep 1
      exchange.publish(payload, routing_key: "#{key}.next")
    end
  end
end

fib(1, 'fib')
