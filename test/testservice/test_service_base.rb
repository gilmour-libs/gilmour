require 'amqp'
require_relative '../../lib/gilmour/base'

class TestServiceBase
  include Gilmour::Base

  def initialize(options)
    initialize_amqp_connection(options)
  end
end
