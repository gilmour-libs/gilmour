require 'securerandom'
require 'gilmour/backends/redis'

def redis_send_and_recv(message, key, ident)
  redis = Gilmour::RedisBackend.new({})
  redis.setup_subscribers({})
  #loop do
    waiter = Thread.new { loop { sleep 1 } }
    newkey = "#{key}.#{SecureRandom.hex(2)}"
    puts "Process: #{ident} Sending: #{newkey}"
    redis.publish(ident, newkey) do |data, code|
      puts "Process: #{ident} Received: #{data}"
      waiter.kill
    end
    waiter.join
  #end
end

def fork_and_run(num)
  pid_array = []

  num.times do |i|
    pid = Process.fork do
      puts "Process #{i}"
      yield i
    end

    pid_array.push(pid)
  end

  pid_array.each do |pid|
    Process.waitpid(pid)
  end

end

fork_and_run(5) do |i|
  redis_send_and_recv('Ping', 'echo', i)
end
