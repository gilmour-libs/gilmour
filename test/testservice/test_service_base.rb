require 'amqp'
require_relative '../../lib/gilmour'

class TestServiceBase
  include Gilmour::Base

  def initialize(options, backend)
    enable_backend(backend, options)
  end
end
