# encoding: utf-8

require 'json'
require 'logger'
require_relative './waiter'

module Gilmour

  # The Responder module that provides the request and respond
  # DSL
  # The public methods in this class are available to be called
  # from the body of the handlers directly
  class Request
    attr_reader :topic, :body

    def initialize(topic, body)
      @topic = topic
      @body = body
    end
  end

  # Every request handler is executed in the context of a Responder
  # object.
  # This class contains methods to respond to requests as well as
  # proxy methods for carrying out gilmour actions inside the handlers.
  class Responder
    LOG_SEPERATOR = '%%'
    LOG_PREFIX = "#{LOG_SEPERATOR}gilmour#{LOG_SEPERATOR}"

    attr_reader :logger
    attr_reader :request
    attr_reader :backend

    def child_logger(writer) #:nodoc:
      logger = Logger.new(STDERR)
      loglevel =  ENV["LOG_LEVEL"] ? ENV["LOG_LEVEL"].to_sym : :warn
      logger.level = Gilmour::LoggerLevels[loglevel] || Logger::WARN
      logger.formatter = proc do |severity, datetime, progname, msg|
        begin
          data = JSON.generate(severity: severity, msg: msg)
          #        data = "#{LOG_PREFIX}#{severity}#{LOG_SEPERATOR}#{msg}"
          writer.write(data+"\n")
          writer.flush
        rescue
          GLogger.error "Logger error: #{severity}: #{msg}"
        end
        nil
      end
      logger
    end

    def make_logger #:nodoc:
      logger = Logger.new(STDERR)
      loglevel =  ENV["LOG_LEVEL"] ? ENV["LOG_LEVEL"].to_sym : :warn
      logger.level = Gilmour::LoggerLevels[loglevel] || Logger::WARN
      logger.formatter = proc do |severity, datetime, progname, msg|
        date_format = datetime.strftime("%Y-%m-%d %H:%M:%S")
        "#{severity[0]} #{date_format} #{@sender} -> #{msg}\n"
      end
      logger
    end

    def initialize(sender, topic, data, backend, opts={}) #:nodoc:
      @sender = sender
      @request = Request.new(topic, data)
      @response = { data: nil, code: nil }
      @backend = backend
      @timeout = opts[:timeout] || 600
      @multi_process = opts[:fork] || false
      @respond = opts[:respond]
      @response_pipe = IO.pipe("UTF-8")
      @logger_pipe = IO.pipe("UTF-8")
      @command_pipe = IO.pipe("UTF-8")
      @logger = make_logger()
      @delayed_response = false
    end

    def receive_data(data) #:nodoc:
      sender, res_data, res_code, opts = JSON.parse(data)
      res_code ||= 200 if @respond
      write_response(sender, res_data, res_code) if sender && res_code
    rescue => e
      GLogger.error e.message
      GLogger.error e.backtrace
    end

    # Called by parent
    def write_response(sender, data, code) #:nodoc:
      return unless @respond
      if code >= 300 && @backend.report_errors?
        emit_error data, code
      end
      @backend.send_response(sender, data, code)
    rescue => e
      GLogger.error e.message
      GLogger.error e.backtrace
    end

    # proxy to base add_listener
    def add_listener(topic, opts={}, &handler)
      if @multi_process
        GLogger.error "Dynamic listeners using add_listener not supported \
        in forked responder. Ignoring!"
      end

      @backend.add_listener(topic, &handler)
    end

    # Proxy to register slot (see Backend#slot for details)
    def slot(topic, opts={}, &handler)
      if @multi_process
        GLogger.error "Dynamic listeners using add_listener not supported \
        in forked responder. Ignoring!"
      end

      @backend.slot(topic, opts, &handler)
    end

    # Proxy to register reply listener (see Backend#reply_to for details)
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

    # This prohibits sending a response from a reply handler even after
    # the request execution has finished. This is useful for sending
    # a response from inside a closure if the handler has to make further
    # gilmour requests. To send a response later, call respond with the
    # option "now" as true.
    def delay_response
      @delayed_response = true
    end

    def delayed_response? #:nodoc:
      @delayed_response
    end

    def command_relay(reader, waiter) #:nodoc:
      waiter.add
      pub_mutex = Mutex.new

      Thread.new do
        loop do
          begin
            data = reader.readline
            pub_mutex.synchronize do
              method, args = JSON.parse(data)
              @backend.send(method.to_sym, *args)
            end
          rescue EOFError
            waiter.done
            break
          rescue Exception => e
            GLogger.debug e.message
            GLogger.debug e.backtrace
          end
        end 
      end 
    end

    # All logs in forked mode are relayed chr
    def logger_relay(read_logger_pipe, waiter, parent_logger) #:nodoc:
      waiter.add 1
      Thread.new do
        loop do
          begin
            data = read_logger_pipe.readline.chomp
            logdata = JSON.parse(data)
            meth = logdata['severity'].downcase.to_sym
            parent_logger.send(meth, logdata['msg'])
          rescue JSON::ParserError
            parent_logger.info data
            next
          rescue EOFError
            waiter.done
            break
          rescue Exception => e
            GLogger.error e.message
            GLogger.error e.backtrace
          end
        end #loop
      end 
    end

    # Called by parent
    def execute(handler) #:nodoc:
      if !@multi_process
        _execute(handler)
        return
      end
      GLogger.debug "Executing #{@sender} in forked moode"

      # Create pipes for child communication
      @read_pipe, @write_pipe = @response_pipe
      @read_command_pipe, @write_command_pipe = @command_pipe
      @read_logger_pipe, @write_logger_pipe = @logger_pipe = IO.pipe("UTF-8")

      # setup relay threads
      wg = Gilmour::Waiter.new
      relay_threads = []
      relay_threads << logger_relay(@read_logger_pipe, wg, @logger)
      relay_threads << command_relay(@read_command_pipe, wg)

      pid = Process.fork do
        @backend.stop
        EventMachine.stop_event_loop

        #Close the parent channels in forked process
        @read_pipe.close
        @read_command_pipe.close
        @read_logger_pipe.close

        @response_sent = false

        # override the logger for the child
        @logger = child_logger(@write_logger_pipe)
        _execute(handler)
        @write_logger_pipe.close
      end

      # Cleanup the writers in Parent process.
      @write_logger_pipe.close
      @write_pipe.close
      @write_command_pipe.close

      begin
        receive_data(@read_pipe.readline)
      rescue EOFError => e
        logger.debug e.message
      end

      pid, status = Process.waitpid2(pid)
      if !status || status.exitstatus > 0
        msg = if !status
                "Child Process #{pid} crashed without status."
              else
                "Child Process #{pid} exited with status #{status.exitstatus}"
              end
        logger.error msg
        # Set the multi-process mode as false, the child has died anyway.
        @multi_process = false
        write_response(@sender, msg, 500)
      end

      @read_pipe.close

      # relay cleanup.
      wg.wait do
        relay_threads.each { |th| th.kill }
      end
      @read_command_pipe.close
      @read_logger_pipe.close
    end

    # Publish all errors on gilmour.error
    # This may or may not have a listener based on the configuration
    # supplied at setup.
    def emit_error(message, code = 500, extra = {}) #:nodoc:
      opts = {
        topic: @request.topic,
        request_data: @request.body,
        userdata: JSON.generate(extra || {}),
        sender: @sender,
        multi_process: @multi_process,
        timestamp: Time.now.getutc
      }

      payload = { backtrace: message, code: code }
      payload.merge!(opts)
      @backend.emit_error payload
    end

    # Called by child
    def _execute(handler) #:nodoc:
      ret = nil
      begin
        Timeout.timeout(@timeout) do
          ret = instance_eval(&handler)
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
      @response[:code] ||= 200 if @respond && !delayed_response?
      send_response if @response[:code]
    end

    def call_parent_backend_method(method, *args) #:nodoc:
      msg = JSON.generate([method, args])
      @write_command_pipe.write(msg+"\n")
      @write_command_pipe.flush
    end

    # Proxy to publish method. See Backend#publish
    def publish(message, destination, opts = {}, code=nil, &blk)
      if @multi_process
        if block_given?
          GLogger.error "Publish callback not supported in forked responder. Ignoring!"
        end
        call_parent_backend_method('publish', message, destination, opts, code)
#        method = opts[:method] || 'publish'
#        msg = JSON.generate([method, [message, destination, opts, code]])
#        @write_command_pipe.write(msg+"\n")
#        @write_command_pipe.flush
      elsif block_given?
        @backend.publish(message, destination, opts, &blk)
      else
        @backend.publish(message, destination, opts)
      end
    end

    # Proxy to request! method. See Backend#request!
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

    # Proxy to signal! method. See Backend#signal!
    def signal!(message, destination, opts={})
      if @multi_process
        call_parent_backend_method('signal!', message, destination, opts)
      else
        @backend.signal!(message, destination, opts)
      end
    end


    # Called by child
    def send_response #:nodoc:
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
