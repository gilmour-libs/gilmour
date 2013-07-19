require 'amqp'
require_relative 'subscriber_base'

class SubscriberBaseClass
  include Gilmour::Base

  def initialize(options)
    initialize_amqp_connection(options)
  end
end
