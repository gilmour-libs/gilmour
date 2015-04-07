# encoding: utf-8

require_relative '../protocol'
module Gilmour
  # Base class for loading backends
  class Backend
    SUPPORTED_BACKENDS = %w(amqp redis)
    @@dir = {}

    def self.implements(token)
      @@dir[token] = self
    end

    def self.get(token)
      @@dir[token]
    end

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

    def add_listener(topic, &handler)
      raise "Not implemented by child class"
    end

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
    end

    def _execute_handler(topic, data, sender, sub)
      Gilmour::Responder.new(sender, topic, data, self).execute(sub[:handler])
    end

    # If optional block is given, it will be passed to the child class
    # implementation of 'send'. The implementation can execute the block
    # on a response to the published message
    def publish(message, destination, opts = {}, code = 0, &blk)
      payload, sender = Gilmour::Protocol.create_request(message, code)
      EM.defer do # Because publish can be called from outside the event loop
        begin
          send(sender, destination, payload, opts, &blk)
        rescue Exception => e
          $stderr.puts e.message
          $stderr.puts e.message
        end
      end
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

