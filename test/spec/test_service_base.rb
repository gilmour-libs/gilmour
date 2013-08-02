# encoding: utf-8

require 'rspec/given'
require 'amqp'

require_relative 'helpers/connection'
require './testservice/test_service_base'

describe TestServiceBase do
  context 'AMQP backend' do
    after(:all) do
      AMQP.stop
      EM.stop
    end
    Given(:subscriber) { TestServiceBase }
    Then { subscriber.should respond_to(:subscribers) }
    Then { subscriber.subscribers.should be_kind_of(Hash) }

    context 'Load existing subscribers' do
      modules_dir = './testservice/subscribers'
      modules = Dir["#{modules_dir}/*.rb"]
      When { subscriber.load_all(modules_dir) }
      Then do
        subscribers = subscriber.subscribers.map do |topic, handlers|
          handlers.map { |handler| handler[:subscriber] }
        end.flatten.uniq
        subscribers.size.should == modules.size
      end
    end
    context 'Connect to AMQP' do
      Given(:subscriber) { TestServiceBase.new(amqp_connection_options) }
      Then { subscriber.backends['amqp'].connection.should be_kind_of AMQP::Session }
      And  { subscriber.backends['amqp'].connection.connected?.should be_true }
      And  { subscriber.backends['amqp'].channel.should be_kind_of AMQP::Channel }
      And  { subscriber.backends['amqp'].exchange.should be_kind_of AMQP::Exchange }
      And  { subscriber.backends['amqp'].exchange.type.should == :topic }
    end
  end
end
