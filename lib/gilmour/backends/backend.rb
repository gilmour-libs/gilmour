# encoding: utf-8

require_relative '../protocol'

module Gilmour
  ErrorChannel = "gilmour.error"

  # Base class for loading backends
  class Backend
    SUPPORTED_BACKENDS = %w(redis)
    @@registry = {}

    attr_accessor :broadcast_errors

    def initialize(opts={})
      if opts["broadcast_errors"] || opts[:broadcast_errors]
        self.broadcast_errors = true
      end
    end

    def self.implements(backend_name)
      @@registry[backend_name] = self
    end

    def self.get(backend_name)
      @@registry[backend_name]
    end

    # :nodoc:
    # This should be implemented by the derived class
    # subscriptions is a hash in the format -
    # { topic => [handler1, handler2, ...],
    #   topic2 => [handler3, handler4, ...],
    #   ...
    # }
    # where handler is a hash
    # { :handler => handler_proc,
    #   :subscriber => subscriber_derived_class
    # }
    def setup_subscribers(subscriptions)
    end

    # Sends a message
    # If optional block is given, it will be executed when a response is received
    # or if timeout occurs
    # +message+:: The body of the message (any object that is serialisable)
    # +destination+:: The channel to post to
    # +opts+::
    #   ++timeout+:: Sender side timeout
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

    def request_destination(dest)
      'gilmour.request.' + dest
    end

    def slot_destination(dest)
      'gilmour.slot.' + dest
    end

    def request!(message, dest, opts = {}, &blk)
      opts[:confirm_subscriber] = true
      publish(message, request_destination(dest), opts, 0, &blk)
    end

    def signal!(message, dest, opts = {})
      GLogger.error('Signal cannot have a callback. Ignoring!') if block_given?
      publish(message, slot_destination(dest), opts)
    end

    def broadcast(message, destination, opts = {}, code = 0, &blk)
      request(message, destination, opts, code, &blk)
      signal(message, destination, opts)
    end

    # Adds a new handler for the given _topic_
    def add_listener(topic, opts={}, &blk)
      raise "Not implemented by child class"
    end

    def listeners(topic)
      raise NotImplementedError.new
    end

    def excl_dups?(topic, opts)
      group = exclusive_group(opts)
      existing = listeners(topic).select { |l| exclusive_group(l) == group }
      !existing.empty?
    end

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
      opts[:excl] = true
      add_listener(req_topic, opts, &blk)
    end

    def slot(topic, options={}, &blk)
      opts = options.dup
      stopic = slot_destination(topic)
      if opts[:excl] && excl_dups?(stopic, opts)
        raise RuntimeError.new("Duplicate reply handler for #{topic}:#{group}")
      end
      opts[:type] = :slot
      #TODO: Check whether topic has a registered subscriber class?
      # or leave it to a linter??
      add_listener(stopic, opts, &blk)
    end

    # Removes existing _handler_ for the _topic_
    def remove_listener(topic, handler)
      raise "Not implemented by child class"
    end

    def remove_slot(topic, handler)
      remove_listener(slot_destination(topic), handler)
    end

    def remove_reply(topic, handler)
      remove_listener(request_destination(topic), handler)
    end

    def acquire_ex_lock(sender)
      raise "Not implemented by child class"
    end

    def send_response(sender, body, code)
      raise "Not implemented by child class"
    end

    def exclusive_group(sub)
      (sub[:excl_group] || sub[:subscriber]).to_s
    end

    def execute_handler(topic, payload, sub)
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

    def _execute_handler(topic, data, sender, sub)
      respond = (sub[:type] != :slot)
      Gilmour::Responder.new(
        sender, topic, data, self, timeout: sub[:timeout],
        fork: sub[:fork], respond: respond
      ).execute(sub[:handler])
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def send_message
      raise "Not implemented by child class"
    end

    def self.load_backend(name)
      require_relative name
    end

    def self.load_all_backends
      SUPPORTED_BACKENDS.each do |f|
        load_backend f
      end
    end

    def stop(sender, body, code)
      raise "Not implemented by child class"
    end

  end
end

