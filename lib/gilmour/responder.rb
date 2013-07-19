# encoding: utf-8

# Top level module
module Gilmour
  # The Responder module that provides the request and respond
  # DSL
  class Responder
    attr_reader :request

    def initialize(topic, data)
      @request = Mash.new({ topic: topic, body: data })
      @response = { data: nil, code: nil }
    end

    def respond(body, code = 200)
      @response[:data] = body
      @response[:code] = code
    end

    def execute(handler)
      Fiber.new do
        instance_eval(&handler)
      end.resume
      [@response[:data], @response[:code]]
    end
  end
end
