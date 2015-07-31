# encoding: utf-8

require 'json'
require 'logger'
require_relative './waiter'

# Top level module
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

  class Responder
    LOG_SEPERATOR = '%%'
    LOG_PREFIX = "#{LOG_SEPERATOR}gilmour#{LOG_SEPERATOR}"

    attr_reader :logger
    attr_reader :request

    def fork_logger(io_writer)
      logger = Logger.new(STDERR)
      loglevel =  ENV["LOG_LEVEL"] ? ENV["LOG_LEVEL"].to_sym : :warn
      logger.level = Gilmour::LoggerLevels[loglevel] || Logger::WARN
      logger.formatter = proc do |severity, datetime, progname, msg|
        data = "#{LOG_PREFIX}#{severity}#{LOG_SEPERATOR}#{msg}"
        io_writer.write(data+"\n")
        io_writer.flush
        nil
      end
      logger
    end

    def make_logger
      logger = Logger.new(STDERR)
      loglevel =  ENV["LOG_LEVEL"] ? ENV["LOG_LEVEL"].to_sym : :warn
      logger.level = Gilmour::LoggerLevels[loglevel] || Logger::WARN
      logger.formatter = proc do |severity, datetime, progname, msg|
        date_format = datetime.strftime("%Y-%m-%d %H:%M:%S")
        "#{severity[0]} #{date_format} #{@sender} -> #{msg}\n"
      end
      logger
    end

    def initialize(sender, topic, data, backend, timeout=600, forked=false)
      @sender = sender
      @request = Request.new(topic, data)
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
      if code >= 300 && @backend.report_errors?
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

    def pub_relay(waiter)
      Thread.new {
        waiter.add 1
        loop {
          begin
            data = @read_publish_pipe.readline
            destination, message = JSON.parse(data)
            @backend.publish(message, destination)
          rescue EOFError
            waiter.done
          rescue Exception => e
            GLogger.debug e.message
            GLogger.debug e.backtrace
          end
        }
      }
    end

    # All logs in forked mode are relayed chr
    def child_io_relay(io_reader, waiter, parent_logger)
      Thread.new {
        waiter.add 1
        loop {
          begin
            data = io_reader.readline.chomp
            if data.start_with?(LOG_PREFIX)
              data.split(LOG_PREFIX).each do |msg|
                unless msg.empty?
                  msg_grp = msg.split(LOG_SEPERATOR, 2)
                  if msg_grp.length > 1
                    data = msg_grp[1]
                    case msg_grp[0]
                    when 'INFO'
                      parent_logger.info data
                    when 'UNKNOWN'
                      parent_logger.unknown data
                    when 'WARN'
                      parent_logger.warn data
                    when 'ERROR'
                      parent_logger.error data
                    when 'FATAL'
                      parent_logger.fatal data
                    else
                      parent_logger.debug data
                    end
                  else
                    parent_logger.debug msg
                  end
                end
              end
              next
            end

            parent_logger.debug data
          rescue EOFError
            waiter.done
          rescue Exception => e
            GLogger.error e.message
            GLogger.error e.backtrace
          end
        }
      }
    end

    # Called by parent
    # :nodoc:
    def execute(handler)
      if @multi_process
        GLogger.debug "Executing #{@sender} in forked moode"

        @read_pipe, @write_pipe = @pipe
        @read_publish_pipe, @write_publish_pipe = @publish_pipe

        io_reader, io_writer = IO.pipe

        wg = Gilmour::Waiter.new
        io_threads = []
        io_threads << child_io_relay(io_reader, wg, @logger)
        io_threads << pub_relay(wg)

        pid = Process.fork do
          @backend.stop
          EventMachine.stop_event_loop

          #Close the parent channels in forked process
          @read_pipe.close
          @read_publish_pipe.close
          io_reader.close unless io_reader.closed?

          @response_sent = false

          @logger = fork_logger(io_writer)
          _execute(handler)
          io_writer.close
        end

        # Cleanup the writers in Parent process.
        io_writer.close
        @write_pipe.close
        @write_publish_pipe.close

        begin
          receive_data(@read_pipe.readline)
        rescue EOFError => e
          logger.debug e.message
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

        @read_pipe.close

        wg.wait do
          io_threads.each { |th|
            th.kill
          }
        end

        # Cleanup.
        @read_publish_pipe.close
        io_reader.close unless io_reader.closed?

      else
        _execute(handler)
      end
    end

    # Publish all errors on gilmour.error
    # This may or may not have a listener based on the configuration
    # supplied at setup.
    def emit_error(message, code = 500, extra = {})
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
