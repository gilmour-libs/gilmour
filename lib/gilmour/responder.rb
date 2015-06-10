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
      @multi_process = backend.multi_process
      @pipe = IO.pipe
    end

    def receive_data(data)
      sender, res_data, res_code = JSON.parse(data)
      write_response(sender, res_data, res_code) if sender && res_code
    end

    # Called by parent
    def write_response(sender, data, code)
      @backend.send_response(sender, data, code)
    end

    def add_listener(topic, &handler)
      @backend.add_listener(topic, &handler)
    end

    def respond(body, code = 200, opts = {})
      @response[:data] = body
      @response[:code] = code
      if opts[:now]
        send_response
        @response = {}
      end
    end

    # Called by parent
    def execute(handler)
      if @multi_process
        @read_pipe = @pipe[0]
        @write_pipe = @pipe[1]

        pid = Process.fork do
          @backend.stop
          EventMachine.stop_event_loop
          @read_pipe.close
          @response_sent = false
          _execute(handler)
        end
        @write_pipe.close
        receive_data(@read_pipe.readline)
        Process.waitpid(pid)
      else
        _execute(handler)
      end
    end

    # Called by child
    def _execute(handler)
      begin
        instance_eval(&handler)
      rescue Exception => e
        $stderr.puts e.message
        $stderr.puts e.backtrace
        @response[:code] = 500
      end
      send_response
      #[@response[:data], @response[:code]]
    end

    # Todo: pipe publisher as well
    def publish(message, destination, opts = {}, &blk)
      @backend.publish(message, destination, opts, &blk)
    end

    # Called by child
    def send_response
      return if @response_sent
      @response_sent = true

      if @multi_process
        msg = JSON.generate([@sender, @response[:data], @response[:code]])
        @write_pipe.write(msg)
        @write_pipe.flush # This flush is very important
      else
        write_response(@sender, @response[:data], @response[:code])
      end
    end
  end
end
