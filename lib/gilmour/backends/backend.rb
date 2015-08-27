# encoding: utf-8
require 'socket'
require 'securerandom'

require_relative '../protocol'
require_relative '../composers'

module Gilmour
  ErrorChannel = "gilmour.error"

  # Base class for loading backends
  class Backend
    SUPPORTED_BACKENDS = %w(redis)
    @@registry = {}

    include Gilmour::Composers
    attr_accessor :broadcast_errors

    def initialize
      @exec_count_lock = Mutex.new
      @exec_count = 0
      @safe_shutdown = false
    end

    def report_errors?  #:nodoc:
      #Override this method to adjust if you want errors to be reported.
      return false
    end

    def register_health_check #:nodoc:
      raise NotImplementedError.new
    end

    def unregister_health_check #:nodoc:
      raise NotImplementedError.new
    end

    def self.implements(backend_name) #:nodoc:
      @@registry[backend_name] = self
    end

    def self.get(backend_name) #:nodoc:
      @@registry[backend_name]
    end

    # This should be implemented by the derived class
    # subscriptions is a hash in the format -
    #   { topic => [handler1, handler2, ...],
    #     topic2 => [handler3, handler4, ...],
    #     ...
    #   }
    # where handler is a hash
    #   { :handler => handler_proc,
    #     :subscriber => subscriber_derived_class
    #   }
    def setup_subscribers(subscriptions) #:nodoc:
    end

    # This is the underlying method for all publishes. Use only if you know
    # what you are doing. Use the request! and signal! methods instead.
    # Sends a message
    # If optional block is given, it will be executed when a response is received
    # or if timeout occurs
    # +message+:: The body of the message (any object that is serialisable)
    # +destination+:: The channel to post to
    # +opts+:: Options
    #          timeout:: Sender side timeout
    #
    def publish(message, destination, opts = {}, code = 0, &blk)
      payload, sender = Gilmour::Protocol.create_request(message, code)

      EM.defer do # Because publish can be called from outside the event loop
        begin
          send_message(sender, destination, payload, opts, &blk)
        rescue Exception => e
          GLogger.debug e.message
          GLogger.debug e.backtrace
        end
      end
      sender
    end

    def request_destination(dest) #:nodoc:
      'gilmour.request.' + dest
    end

    def slot_destination(dest) #:nodoc:
      'gilmour.slot.' + dest
    end

    # Sends a request
    # If optional block is given, it will be executed when a response is received
    # or if timeout occurs. You usually want to provide this (otherwise consider
    # using slots)
    # Params:
    # +message+:: The body of the message (any object that is serialisable)
    # +destination+:: The channel to post to
    # +opts+:: Options
    #          timeout:: Sender side timeout
    #          confirm_subscriber:: Confirms that active subscriber exists,
    #          else calls blk with 404 error code.
    def request!(message, dest, opts = {}, &blk)
      opts[:confirm_subscriber] = true
      publish(message, request_destination(dest), opts, 0, &blk)
    end

    # Emits a signal
    # Params:
    # +message+:: The body of the message (any object that is serialisable)
    # +destination+:: The channel to post to
    def signal!(message, dest, opts = {})
      GLogger.error('Signal cannot have a callback. Ignoring!') if block_given?
      publish(message, slot_destination(dest), opts)
    end

    # Emits a signal and sends a request
    def broadcast(message, destination, opts = {}, code = 0, &blk)
      request(message, destination, opts, code, &blk)
      signal(message, destination, opts)
    end

    def emit_error(message)
      raise NotImplementedError.new
    end

    # Adds a new handler for the given _topic_
    def add_listener(topic, opts={}, &blk)
      raise "Not implemented by child class"
    end

    def listeners(topic) #:nodoc:
      raise NotImplementedError.new
    end

    def excl_dups?(topic, opts) #:nodoc:
      group = exclusive_group(opts)
      existing = listeners(topic).select { |l| exclusive_group(l) == group }
      !existing.empty?
    end

    # Sets up a reply listener
    # Params:
    # +topic+:: The topic to listen on
    # +options+:: Options Hash
    #             timeout:: a 504 error code is sent after this time and the
    #             execution for the request is terminated
    #             fork:: The request will be processed in a forked process.
    #             useful for long running executions
    #             excl_group:: The exclustion group for this handler (see README for explanation)
    # +blk+:: The block to the executed for the request
    def reply_to(topic, options={}, &blk)
      opts = options.dup
      group = exclusive_group(opts)
      if group.empty?
        group = "_default"
        opts[:excl_group] = group
        GLogger.warn("Using default exclusion group for #{topic}")
      end
      req_topic = request_destination(topic)
      if excl_dups?(req_topic, opts)
        raise RuntimeError.new("Duplicate reply handler for #{topic}:#{group}")
      end
      opts[:type] = :reply
      opts[:exclusive] = true
      add_listener(req_topic, opts, &blk)
    end

    # Sets up a slot listener
    # Params:
    # +topic+:: The topic to listen on
    # +options+:: Options hash
    #             timeout:: a 504 error code is sent after this time and the
    #             execution for the request is terminated
    #             fork:: The request will be processed in a forked process.
    #             useful for long running executions
    # +blk+:: The block to the executed for the request
    def slot(topic, options={}, &blk)
      opts = options.dup
      stopic = slot_destination(topic)
      if opts[:exclusive] && excl_dups?(stopic, opts)
        raise RuntimeError.new("Duplicate reply handler for #{topic}:#{group}")
      end
      opts[:type] = :slot
      #TODO: Check whether topic has a registered subscriber class?
      # or leave it to a linter??
      add_listener(stopic, opts, &blk)
    end

    # Removes existing _handler_ for the _topic_
    # Use only if you have registered the handler with add_listener
    # or listen_to
    def remove_listener(topic, handler)
      raise "Not implemented by child class"
    end

    # Removes existing slot handler for the _topic_
    def remove_slot(topic, handler)
      remove_listener(slot_destination(topic), handler)
    end

    # Removes existing reply handler for the _topic_
    def remove_reply(topic, handler)
      remove_listener(request_destination(topic), handler)
    end

    def acquire_ex_lock(sender) #:nodoc:
      raise "Not implemented by child class"
    end

    def send_response(sender, body, code) #:nodoc:
      raise "Not implemented by child class"
    end

    def exclusive_group(sub) #:nodoc:
      (sub[:excl_group] || sub[:subscriber]).to_s
    end

    def execute_handler(topic, payload, sub) #:nodoc:
      data, sender = Gilmour::Protocol.parse_request(payload)
      if sub[:exclusive]
        group = exclusive_group(sub)
        if group.empty?
          raise RuntimeError.new("Exclusive flag without group encountered!")
        end
        lock_key = sender + group
        acquire_ex_lock(lock_key) { _execute_handler(topic, data, sender, sub) }
      else
        _execute_handler(topic, data, sender, sub)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def _execute_handler(topic, data, sender, sub) #:nodoc:
      if @safe_shutdown
        return
      end
      @exec_count_lock.synchronize { @exec_count += 1 }
      respond = (sub[:type] != :slot)
      Gilmour::Responder.new(
        sender, topic, data, self, timeout: sub[:timeout],
        fork: sub[:fork], respond: respond
      ).execute(sub[:handler])
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    ensure
      @exec_count_lock.synchronize { @exec_count -= 1 }
    end

    def safe_shutdown(block=true, poll_delay=30)
      pause
      @safe_shutdown = true
      $stderr.puts "Shutting down!"
      loop do
        #@exec_count_lock.synchronize do
        if @exec_count == 0
          stop
          sleep poll_delay
          return
        end
        #end
        $stderr.puts "Active requests: #{@exec_count}"
        sleep poll_delay
      end
    rescue => e
      $stderr.puts e.message
      $stderr.puts e.backtrace
    end

    def send_message #:nodoc:
      raise "Not implemented by child class"
    end

    def self.load_backend(name) #:nodoc:
      require_relative name
    end

    def self.load_all_backends #:nodoc:
      SUPPORTED_BACKENDS.each do |f|
        load_backend f
      end
    end

    # stop the backend
    def stop
      raise "Not implemented by child class"
    end

    def pause
      raise NotImplementedError.new
    end
  end
end

