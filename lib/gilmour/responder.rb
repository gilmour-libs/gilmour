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
        $stderr.puts "Receive: #{data}"
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

    def execute(handler)
      @read_pipe = EventMachine.attach(@pipe[0], Plumber, self)
      EventMachine.fork_reactor do
        @read_pipe.close_connection
        @write_pipe = EventMachine.attach(@pipe[1])
        _execute(handler)
      end
    end

    def _execute(handler)
      begin
        instance_eval(&handler)
      rescue Exception => e
        $stderr.puts e.message  
        $stderr.puts e.backtrace  
        @response[:code] = 500
      end
      send_response if @response[:code] && @sender
      [@response[:data], @response[:code]]
    end

    def publish(message, destination, opts = {}, &blk)
      @backend.publish(message, destination, opts, &blk)
    end

    def send_response
      msg = JSON.generate([@sender, @response[:data], @response[:code]])
      @write_pipe.send_data(msg)
    end

    def send(sender, data, code)
      @backend.send_response(sender, data, code)
    end
  end
end
