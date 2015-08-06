require 'gilmour'

class Server
  include Gilmour::Base
end
server = Server.new
gilmour = server.enable_backend('redis')

gilmour.reply_to "fib", excl_group: 'fib', timeout: 1 do
  first = request.body['first']
  second = request.body['second']
  respond('next' => first + second)
end

handler = proc do |first, second|
  first ||= 1
  second ||= 1
  gilmour.request!({ 'first' => first, 'second' => second }, 'fib',
                   timeout: 5) do |resp, code|
    if code != 200
      $stderr.puts "Something went wrong in the response! Aborting"
      exit
    end
    next_val = resp['next']
    puts next_val
    EM.add_timer(1) { handler.call(second, resp['next']) }
  end
end

EM.next_tick { handler.call }
server.start true

