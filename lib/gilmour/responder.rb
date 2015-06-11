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
      @publish_pipe = IO.pipe
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

        @read_publish_pipe = @publish_pipe[0]
        @write_publish_pipe = @publish_pipe[1]

        pid = Process.fork do
          @backend.stop
          EventMachine.stop_event_loop
          @read_pipe.close
          @read_publish_pipe.close
          @response_sent = false
          _execute(handler)
        end

        @write_pipe.close
        @write_publish_pipe.close

        pub_mutex = Mutex.new

        pub_reader = Thread.new {
          loop {
            begin
              data = @read_publish_pipe.readline
              pub_mutex.synchronize do
                destination, message = JSON.parse(data)
                @backend.publish(message, destination)
              end
            rescue EOFError => e
            end
          }
        }

        begin
          receive_data(@read_pipe.readline)
        rescue EOFError => e
          $stderr.puts e.message
          $stderr.puts "EOFError caught in responder.rb, because of nil response"
        end

        Process.waitpid(pid)

        pub_mutex.synchronize do
          pub_reader.kill
        end

        @read_pipe.close
        @read_publish_pipe.close
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
      send_response if @response[:code]
      #[@response[:data], @response[:code]]
    end

    # Todo: pipe publisher as well
    def publish(message, destination, opts = {})
      if @multi_process
        if block_given?
          raise Exception.new("Publish Callback is not supported in forked mode.")
        end

        msg = JSON.generate([destination, message])
        @write_publish_pipe.write(msg+"\n")
        @write_publish_pipe.flush
      elsif block_given?
        blk = Proc.new
        @backend.publish(message, destination, opts, &blk)
      else
        @backend.publish(message, destination, opts)
      end
    end

    # Called by child
    def send_response
      return if @response_sent
      @response_sent = true

      if @multi_process
        msg = JSON.generate([@sender, @response[:data], @response[:code]])
        @write_pipe.write(msg+"\n")
        @write_pipe.flush # This flush is very important
      else
        write_response(@sender, @response[:data], @response[:code])
      end
    end
  end
end
