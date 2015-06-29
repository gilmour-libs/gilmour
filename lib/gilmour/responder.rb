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

    def make_logger
      logger = Logger.new(STDERR)
      original_formatter = Logger::Formatter.new
      loglevel =  ENV["LOG_LEVEL"] ? ENV["LOG_LEVEL"].to_sym : :warn
      logger.level = Gilmour::LoggerLevels[loglevel] || Logger::WARN
      logger.formatter = proc do |severity, datetime, progname, msg|
        original_formatter.call(severity, datetime, @sender, msg)
      end
      logger
    end

    def initialize(sender, topic, data, backend, timeout=600, forked=false)
      @sender = sender
      @request = Mash.new(topic: topic, body: data)
      @response = { data: nil, code: nil }
      @backend = backend
      @timeout = timeout || 600
      @multi_process = forked || false
      @pipe = IO.pipe
      @publish_pipe = IO.pipe
      @logger = make_logger()
    end

    def receive_data(data)
      sender, res_data, res_code = JSON.parse(data)
      write_response(sender, res_data, res_code) if sender && res_code
    end

    # Called by parent
    def write_response(sender, data, code)
      if code > 200
        emit_error data, code
      end

      @backend.send_response(sender, data, code)
    end

    # Adds a dynamic listener for _topic_
    def add_listener(topic, &handler)
      if @multi_process
        GLogger.error "Dynamic listeners using add_listener not supported \
        in forked responder. Ignoring!"
      end

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
    def execute(handler)
      if @multi_process
        GLogger.debug "Executing #{@sender} in forked moode"
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

        pid, status = Process.waitpid2(pid)
        if !status
          msg = "Child Process #{pid} crashed without status."
          logger.error msg
          # Set the multi-process mode as false, the child has died anyway.
          @multi_process = false
          write_response(@sender, msg, 500)
        elsif status.exitstatus > 0
          msg = "Child Process #{pid} exited with status #{status.exitstatus}"
          logger.error msg
          # Set the multi-process mode as false, the child has died anyway.
          @multi_process = false
          write_response(@sender, msg, 500)
        end

        pub_mutex.synchronize do
          pub_reader.kill
        end

        @read_pipe.close
        @read_publish_pipe.close
      else
        _execute(handler)
      end
    end

    def emit_error(message, code=500, extra={})
      # Publish all errors on gilmour.error
      # This may or may not have a listener based on the configuration
      # supplied at setup.
      opts = {
        :topic => @request[:topic],
        :data => @request[:data],
        :description => '',
        :sender => @sender,
        :multi_process => @multi_process,
        :code => 500
      }.merge(extra || {})

      opts[:timestamp] = Time.now.getutc
      payload = {:traceback => message, :extra => opts, :code => code}
      @backend.emit_error payload
    end

    # Called by child
    # :nodoc:
    def _execute(handler)
      begin
        Timeout.timeout(@timeout) do
          instance_eval(&handler)
        end
      rescue Timeout::Error => e
        logger.error e.message
        logger.error e.backtrace
        @response[:code] = 504
        @response[:data] = e.message
      rescue Exception => e
        logger.error e.message
        logger.error e.backtrace
        @response[:code] = 500
        @response[:data] = e.message
      end

      send_response if @response[:code]
    end

    # Publishes a message. See Backend::publish
    def publish(message, destination, opts = {}, code=nil)
      if @multi_process
        if block_given?
          GLogger.error "Publish callback not supported in forked responder. Ignoring!"
#          raise Exception.new("Publish Callback is not supported in forked mode.")
        end

        msg = JSON.generate([destination, message, code])
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
