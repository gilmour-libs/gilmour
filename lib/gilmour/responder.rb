# encoding: utf-8

require "logger"

# Top level module
module Gilmour
  # The Responder module that provides the request and respond
  # DSL
  # The public methods in this class are available to be called
  # from the body of the handlers directly

  class Responder
    attr_reader :logger
    attr_reader :request

    def getLogger
      logger = Logger.new(STDERR)
      original_formatter = Logger::Formatter.new
      logger.formatter = proc do |severity, datetime, progname, msg|
        log_msg = original_formatter.call(severity, datetime, @sender, msg)
        @log_stack.push(log_msg)
        log_msg
      end
      logger
    end

    def initialize(sender, topic, data, backend)
      @sender = sender
      @request = Mash.new(topic: topic, body: data)
      @response = { data: nil, code: nil }
      @backend = backend
      @multi_process = backend.multi_process
      @pipe = IO.pipe
      @publish_pipe = IO.pipe
      @log_stack = []
      @logger = getLogger()
    end

    def receive_data(data)
      sender, res_data, res_code = JSON.parse(data)
      write_response(sender, res_data, res_code) if sender && res_code
    end

    # Called by parent
    def write_response(sender, data, code)
      @backend.send_response(sender, data, code)
    end

    # Adds a dynamic listener for _topic_
    def add_listener(topic, &handler)
      @backend.add_listener(topic, &handler)
    end

    # Sends a response with _body_ and _code_
    # If +opts[:now]+ is true, the response is sent immediately,
    # else it is defered until the handler finishes executing
    def respond(body, code = 200, opts = {})
      @response[:data] = body
      @response[:code] = code
      if opts[:now]
        send_response
        @response = {}
      end
    end

    # Called by parent
    # :nodoc:
    def execute(handler, timeout)
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
          _execute(handler, timeout)
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
            rescue EOFError
              # awkward blank rescue block
            end
          }
        }

        begin
          receive_data(@read_pipe.readline)
        rescue EOFError => e
          logger.debug e.message
          logger.debug "EOFError caught in responder.rb, because of nil response"
        end

        Process.waitpid(pid)

        pub_mutex.synchronize do
          pub_reader.kill
        end

        @read_pipe.close
        @read_publish_pipe.close
      else
        _execute(handler, timeout)
      end
    end

    def emit_error
      @backend.emit_error @log_stack
    end

    # Called by child
    # :nodoc:
    def _execute(handler, timeout=600)
      begin
        Timeout.timeout(timeout) do
          instance_eval(&handler)
        end
      rescue Timeout::Error => e
        logger.error e.message
        logger.warn e.backtrace
        @response[:code] = 409
        emit_error
      rescue Exception => e
        logger.info e.message
        logger.info e.backtrace
        @response[:code] = 500
        emit_error
      end
      send_response if @response[:code]
    end

    # Publishes a message. See Backend::publish
    def publish(message, destination, opts = {})
      if @multi_process
        if block_given?
          logger.error "Publish callback not supported in forked responder. Ignoring!"
#          raise Exception.new("Publish Callback is not supported in forked mode.")
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
    # :nodoc:
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
