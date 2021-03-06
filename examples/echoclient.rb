require 'securerandom'
require 'gilmour'

class Server
  include Gilmour::Base
end


def redis_send_and_recv(message, key)
  server = Server.new
  gilmour = server.enable_backend('redis')
  count = 0
  loop do
    waiter = Gilmour::Waiter.new
    gilmour.request!(count, key) do |data, code|
      puts "Client got response: #{code}: #{data}"
      waiter.signal
    end
    count = count + 1
    waiter.wait
    sleep 1
  end
end

redis_send_and_recv('Ping', 'echo')

