# encoding: utf-8
require 'gilmour'

class EventServer
  include Gilmour::Base

  def initialize
    enable_backend('redis')
    enable_backend('amqp', { host: 'localhost', exchange: 'fib' })
    start(true)
  end
end

class EchoSubscriber < EventServer
  self.backend = 'redis'

  listen_to 'echo.*' do
    if request.body == 'Palmolive'
      respond nil
    else
      respond "#{request.topic}"
    end
  end
end

class FibonacciSubscriber < EventServer
  self.backend = 'amqp'
  
  class << self
    attr_accessor :last
  end

  listen_to 'fib.next' do
    old = FibonacciSubscriber.last
    FibonacciSubscriber.last = new = request.body
    respond(old + new)
  end

  listen_to 'fib.init' do
    FibonacciSubscriber.last = request.body
  end

end

EventServer.new
