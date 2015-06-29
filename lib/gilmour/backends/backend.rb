# encoding: utf-8
require 'socket'
require 'securerandom'

require_relative '../protocol'

module Gilmour
  ErrorChannel = "gilmour.error"

  # Base class for loading backends
  class Backend
    SUPPORTED_BACKENDS = %w(redis)
    @@registry = {}

    def ident
      @ident
    end

    def generate_ident
      "#{Socket.gethostname}-pid-#{Process.pid}-uuid-#{SecureRandom.uuid}"
    end

    def report_errors?
      #Override this method to adjust if you want errors to be reported.
      return true
    end

    def initialize(opts={})
      @ident = generate_ident
    end

    def register_health_check
      raise NotImplementedError.new
    end

    def unregister_health_check
      raise NotImplementedError.new
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
          send(sender, destination, payload, opts, &blk)
        rescue Exception => e
          GLogger.debug e.message
          GLogger.debug e.backtrace
        end
      end
      sender
    end

    def emit_error(message)
      raise NotImplementedError.new
    end

    # Adds a new handler for the given _topic_
    def add_listener(topic, &handler)
      raise "Not implemented by child class"
    end

    # Removes existing _handler_ for the _topic_
    def remove_listener(topic, &handler)
      raise "Not implemented by child class"
    end

    def acquire_ex_lock(sender)
      raise "Not implemented by child class"
    end

    def send_response(sender, body, code)
      raise "Not implemented by child class"
    end

    def execute_handler(topic, payload, sub)
      data, sender = Gilmour::Protocol.parse_request(payload)
      if sub[:exclusive]
        lock_key = sender + sub[:subscriber].to_s
        acquire_ex_lock(lock_key) { _execute_handler(topic, data, sender, sub) }
      else
        _execute_handler(topic, data, sender, sub)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def _execute_handler(topic, data, sender, sub)
      Gilmour::Responder.new(
        sender, topic, data, self, sub[:timeout], sub[:fork]
      ).execute(sub[:handler])
    end

    def send
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

