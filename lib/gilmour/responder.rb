# encoding: utf-8

# Top level module
module Gilmour
  # The Responder module that provides the request and respond
  # DSL
  class Responder
    attr_reader :request

    def initialize(sender, topic, data, backend)
      @sender = sender
      @request = Mash.new(topic: topic, body: data)
      @response = { data: nil, code: nil }
      @backend = backend
      @pipe = IO.pipe
    end

    class Plumber < EventMachine::Connection
      def initialize(sender)
        $stderr.puts "Sender: #{sender}"
        @sender = sender
      end

      def receive_data(data)
        $stderr.puts "receive_data: #{data}"
        sender, res_data, res_code = JSON.parse(data)
        @sender.send(sender, res_data, res_code)
      end
    end

    def add_listener(topic, &handler)
      @backend.add_listener(topic, &handler)
    end

    def respond(body, code = 200, opts = {})
      @response[:data] = body
      @response[:code] = code
      if opts[:now]
        send_response if @sender
        @response = {}
      end
    end

    def receive_data(data)
      $stderr.puts "receive_data: #{data}"
      sender, res_data, res_code = JSON.parse(data)
      write_response(sender, res_data, res_code) if res_code && sender
    end

    def execute(handler)
      $stderr.puts "Forking -->"
      @read_pipe = @pipe[0]
      @reader = Thread.new do
        receive_data(@read_pipe.readline)
        @read_pipe.close
      end
      @write_pipe = @pipe[1]
      pid = Process.fork do
        $stderr.puts "In child"
        @read_pipe.close
        _execute(handler)
        $stderr.puts "Child done"
      end
      @write_pipe.close
      Process.waitpid(pid)
      @reader.join
      $stderr.puts "Parent done"
    end

    def _execute(handler)
      begin
        instance_eval(&handler)
      rescue Exception => e
        $stderr.puts e.message  
        $stderr.puts e.backtrace  
        @response[:code] = 500
      end
      send_response
      [@response[:data], @response[:code]]
    end

    def publish(message, destination, opts = {}, &blk)
      @backend.publish(message, destination, opts, &blk)
    end

    def send_response
      msg = JSON.generate([@sender, @response[:data], @response[:code]])
      @write_pipe.write(msg)
      @write_pipe.flush
    end

    def write_response(sender, data, code)
      @backend.send_response(sender, data, code)
      $stderr.puts "Sent response"
    end
  end
end
