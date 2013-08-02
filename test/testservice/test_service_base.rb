require 'amqp'
require_relative '../../lib/gilmour/base'

class TestServiceBase
  include Gilmour::Base

  def initialize(options)
    enable_backend('amqp', options)
  end
end
