# encoding: utf-8
require_relative '../lib/gilmour'

class EventServer
  include Gilmour::Base

  def initialize
    backend = 'redis'
    enable_backend(backend, { })
    registered_subscribers.each do |sub|
      sub.backend = backend
    end
    $stderr.puts "Starting server. To see messaging in action run clients."
    start(true)
  end
end

class EchoSubscriber < EventServer
  class << self
    def capture_output(pipe)
      streams = [$stdout, $stderr]

      # Save the streams to be reassigned later.
      # Actually it doesn't matter because the child process would be killed
      # anyway after the work is done.
      saved = streams.collect { |stream| stream.dup }

      begin
        streams.each_with_index do |stream, ix|
          # Probably I should not use IX, otherwise stdout and stderr can arrive
          # out of order, which they should?
          # If I reopen both of them on the same PIPE, they are guaranteed to
          # arrive in order.
          stream.reopen(pipe)
          #stream.sync = true
        end
        yield
      ensure
        # This is sort of meaningless, just makes sense aesthetically.
        # To return what was borrowed.
        streams.each_with_index do |stream, i|
          stream.reopen(saved[i])
        end
        pipe.close unless pipe.closed?
      end
    end

    def ds_respond(topic, opts={}, &blk)
      options = { exclusive: true, fork: true }.merge(opts)
      listen_to topic, options do
        logger.error "Captuting output before execution"

        waiter = Gilmour::Waiter.new
        waiter.add 1
        read_pipe, write_pipe = IO.pipe

        th = Thread.new {
          loop {
            begin
              result = read_pipe.readline.chomp
              logger.debug result
            rescue EOFError
              waiter.done
            rescue Exception => e
              logger.error "Error: #{e.message}"
              logger.error "Traceback: #{e.backtrace}"
            end
          }
        }

        EchoSubscriber::capture_output(write_pipe) do
          instance_eval(&blk)
        end

        waiter.wait do
          th.kill
        end

      end
    end
  end

  # Passing second parameter as true makes only one instance of this handler handle a request
  EchoSubscriber::ds_respond 'echo.*' do
    if request.body == 'Palmolive'
      respond nil
    else
      logger.error "logger: #{request.body}"
      $stderr.puts "stderr.puts: #{request.body}"
      puts "stdout.puts #{request.body}"
      respond "#{request.topic}"
    end
  end

end

EventServer.new
