# encoding: utf-8
# Example of running as a container with subscribers
# Run ruby server.rb first, followed by echoclient.rb
require 'gilmour'

class EventServer
  include Gilmour::Base

  def initialize
    enable_backend('redis')
    registered_subscribers.each do |sub|
      sub.backend = 'redis'
    end
    start(true)
  end
  load_all './subscribers'
end

EventServer.new
