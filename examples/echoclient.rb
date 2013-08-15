require 'securerandom'
require 'gilmour/backends/redis'

def redis_send_and_recv(message, key)
  redis = Gilmour::RedisBackend.new({})
  redis.setup_subscribers({})
  loop do
    waiter = Thread.new { loop { sleep 1 } }
    newkey = "#{key}.#{SecureRandom.hex(2)}"
    redis.publish(message, newkey) do |data, code|
      puts "Client got response: #{code}: #{data}"
      waiter.kill
    end
    waiter.join
    sleep 1
  end
end

redis_send_and_recv('Ping', 'echo')

