# encoding: utf-8
require 'gilmour'

class EventServer
  include Gilmour::Base

  def initialize
    enable_backend('redis', { })
    registered_subscribers.each do |sub|
      sub.backend = backend
    end
    $stderr.puts "Starting server. To see messaging in action run clients."
    start(true)
  end
end

class EchoSubscriber < EventServer
  # Passing second parameter as true makes only one instance of this handler handle a request
  listen_to 'echo.*', true do
    if request.body == 'Palmolive'
      respond nil
    else
      $stderr.puts request.body
      respond "#{request.topic}"
    end
  end
end

class FibonacciSubscriber < EventServer
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
