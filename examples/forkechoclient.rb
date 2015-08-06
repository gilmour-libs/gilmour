require 'securerandom'
require 'gilmour'

class Server
  include Gilmour::Base
end

def redis_send_and_recv(message, key)
  server = Server.new
  gilmour = server.enable_backend('redis')
  loop do
    waiter = Gilmour::Waiter.new
    puts "Process: Sending: #{key}"
    gilmour.request!(message, key) do |data, code|
      puts "Process: Received: #{data}"
      waiter.signal
    end
    waiter.wait
    sleep 1
  end
end

redis_send_and_recv('Ping', 'forkecho')
