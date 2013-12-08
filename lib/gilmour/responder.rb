# encoding: utf-8

# Top level module
module Gilmour
  # The Responder module that provides the request and respond
  # DSL
  class Responder
    attr_reader :request

    def initialize(topic, data, backend)
      @request = Mash.new(topic: topic, body: data)
      @response = { data: nil, code: nil }
      @backend = backend
    end

    def add_listener(topic, &handler)
      @backend.add_listener(topic, &handler)
    end

    def respond(body, code = 200)
      @response[:data] = body
      @response[:code] = code
    end

    def execute(handler)
      Fiber.new do
        begin
          instance_eval(&handler)
        rescue
          nil
        end
      end.resume
      [@response[:data], @response[:code]]
    end

    def publish(message, destination, &blk)
      @backend.publish(message, destination, nil, &blk)
    end
  end
end
