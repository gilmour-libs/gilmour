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

    def publish(message, key)
    end

    def self.load_backend(name)
      require_relative name
    end

    def self.load_all_backends
      SUPPORTED_BACKENDS.each do |f|
        load_backend f
      end
    end
  end
end

