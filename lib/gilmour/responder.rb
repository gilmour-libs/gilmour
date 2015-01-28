# encoding: utf-8

# Top level module
module Gilmour
  # The Responder module that provides the request and respond
  # DSL
  class Responder
    attr_reader :request

    def initialize(sender, topic, data, backend)
      @sender = sender
      @request = Mash.new(topic: topic, body: data)
      @response = { data: nil, code: nil }
      @backend = backend
    end

    def add_listener(topic, &handler)
      @backend.add_listener(topic, &handler)
    end

    def respond(body, code = 200, opts = {})
      @response[:data] = body
      @response[:code] = code
      if opts[:now]
        send_response if @sender
        @response = {}
      end
    end

    def execute(handler)
      begin
        instance_eval(&handler)
      rescue Exception => e
        $stderr.puts e.message  
        $stderr.puts e.backtrace  
        @response[:code] = 500
      end
      send_response if @response[:code] && @sender
      [@response[:data], @response[:code]]
    end

    def publish(message, destination, opts = {}, &blk)
      @backend.publish(message, destination, opts, &blk)
    end

    def send_response
      @backend.send_response(@sender, @response[:data], @response[:code])
    end
  end
end
