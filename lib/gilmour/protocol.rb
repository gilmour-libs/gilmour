# encoding: utf-8
# Top level Gilmour module
module Gilmour
  # This module implements the JSON communication protocol
  module Protocol
    def self.parse_request(payload)
      payload = sanitised_payload(payload)
      data = sender = nil
      if payload.kind_of? Hash
        data = payload['data']
        sender = payload['sender']
      else
        data = payload
      end
      [data, sender]
    end

    def self.parse_response(payload)
      payload = sanitised_payload(payload)
      data = sender = nil
      if payload.kind_of? Hash
        data = payload['data']
        sender = payload['sender']
        code = payload['code']
      else
        data = payload
      end
      [data, code, sender]
    end

    def self.create_request(data, code = nil, sender = nil)
      sender ||= SecureRandom.hex
      payload = JSON.generate({ 'data' => data, 'code' => code,
                              'sender' => sender })
      [payload, sender]
    end

    def self.sanitised_payload(raw)
      ret = begin
              JSON.parse(raw)
            rescue
              raw
            end
      ret
    end
  end
end
