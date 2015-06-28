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
        log_msg = original_formatter.call(severity, datetime, @sender, msg)
        @log_stack.push(log_msg)
        log_msg
      end
      logger
    end

    def initialize(sender, topic, data, backend, opts={})
      @sender = sender
      @request = Mash.new(topic: topic, body: data)
      @response = { data: nil, code: nil }
      @backend = backend
      @timeout = opts[:timeout] || 600
      @multi_process = opts[:fork] || false
      @respond = opts[:respond]
      @pipe = IO.pipe
      @publish_pipe = IO.pipe
      @log_stack = []
      @logger = make_logger()
      @delayed_response = false
    end

    def receive_data(data)
      sender, res_data, res_code, opts = JSON.parse(data)
      res_code ||= 200 if @respond
      write_response(sender, res_data, res_code) if sender && res_code
    end

    # Called by parent
    def write_response(sender, data, code)
      return unless @respond
      @backend.send_response(sender, data, code)
    end

    # Adds a dynamic listener for _topic_
    def add_listener(topic, opts={}, &handler)
      if @multi_process
        GLogger.error "Dynamic listeners using add_listener not supported \
        in forked responder. Ignoring!"
      end

      @backend.add_listener(topic, &handler)
    end

    def slot(topic, opts={}, &handler)
      if @multi_process
        GLogger.error "Dynamic listeners using add_listener not supported \
        in forked responder. Ignoring!"
      end

      @backend.slot(topic, opts, &handler)
    end

    def reply_to(topic, opts={}, &handler)
      if @multi_process
        GLogger.error "Dynamic listeners using add_listener not supported \
        in forked responder. Ignoring!"
      end

      @backend.reply_to(topic, opts, &handler)
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

    def delay_response
      @delayed_response = true
    end

    def delayed_response?
      @delayed_response
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
                method, args = JSON.parse(data)
                @backend.send(method.to_sym, *args)
              end
            rescue EOFError
              # awkward blank rescue block
            rescue Exception => e
              GLogger.debug e.message
              GLogger.debug e.backtrace
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
          emit_error :description => msg
          write_response(@sender, msg, 500)
        elsif status.exitstatus > 0
          msg = "Child Process #{pid} exited with status #{status.exitstatus}"
          logger.error msg
          # Set the multi-process mode as false, the child has died anyway.
          @multi_process = false
          emit_error :description => msg
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

    def emit_error(extra={})
      opts = {
        :topic => @request[:topic],
        :description => '',
        :sender => @sender,
        :multi_process => @multi_process,
        :code => 500
      }.merge(extra || {})

      # Publish all errors on gilmour.error
      # This may or may not have a listener based on the configuration
      # supplied at setup.
      if @backend.broadcast_errors
        opts[:timestamp] = Time.now.getutc
        payload = {:traceback => @log_stack, :extra => opts}
        publish(payload, Gilmour::ErrorChannel, {}, 500)
      end
    end

    # Called by child
    # :nodoc:
    def _execute(handler)
      ret = nil
      begin
        Timeout.timeout(@timeout) do
          ret = instance_eval(&handler)
        end
      rescue Timeout::Error => e
        logger.error e.message
        logger.warn e.backtrace
        @response[:code] = 504
        emit_error :code => 504, :description => e.message
      rescue Exception => e
        logger.debug e.message
        logger.debug e.backtrace
        @response[:code] = 500
        emit_error :description => e.message
      end
      @response[:code] ||= 200 if @respond && !delayed_response?
      send_response if @response[:code]
    end

    def call_parent_backend_method(method, *args)
      msg = JSON.generate([method, args])
      @write_publish_pipe.write(msg+"\n")
      @write_publish_pipe.flush
    end

    # Publishes a message. See Backend::publish
    def publish(message, destination, opts = {}, code=nil, &blk)
      if @multi_process
        if block_given?
          GLogger.error "Publish callback not supported in forked responder. Ignoring!"
        end
        call_parent_backend_method('publish', message, destination, opts, code)
#        method = opts[:method] || 'publish'
#        msg = JSON.generate([method, [message, destination, opts, code]])
#        @write_publish_pipe.write(msg+"\n")
#        @write_publish_pipe.flush
      elsif block_given?
        @backend.publish(message, destination, opts, &blk)
      else
        @backend.publish(message, destination, opts)
      end
    end

    def request!(message, destination, opts={}, &blk)
      if @multi_process
        if block_given?
          GLogger.error "Publish callback not supported in forked responder. Ignoring!"
        end
        call_parent_backend_method('request!', message, destination, opts)
      else
        @backend.request!(message, destination, opts, &blk)
      end
    end

    def signal!(message, destination, opts={})
      if @multi_process
        call_parent_backend_method('signal!', message, destination, opts)
      else
        @backend.signal!(message, destination, opts, &blk)
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
