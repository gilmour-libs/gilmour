require 'rspec/given'
require 'amqp'

require_relative 'helpers/connection'
require './testservice/test_service_base'

describe TestServiceBase do
  after(:all) { AMQP.stop; EM.stop }
  Given(:subscriber) { TestServiceBase }
  Then { subscriber.should respond_to(:subscribers) }
  Then { subscriber.subscribers.should be_kind_of(Hash) }

  context 'Load existing subscribers' do
    ModulesDir = './testservice/subscribers'
    modules = Dir["#{ModulesDir}/*.rb"]
    When { subscriber.load_all(ModulesDir) }
    Then do
      subscribers = subscriber.subscribers.map do |topic, handlers|
        handlers.map { |handler| handler[:subscriber] }
      end.flatten.uniq
      subscribers.size.should == modules.size
    end
  end
  context 'Connect to AMQP' do
    Given(:subscriber) { TestServiceBase.new(amqp_connection_options) }
    Then { subscriber.connection.should be_kind_of AMQP::Session }
    And  { subscriber.connection.connected?.should be_true }
    And  { subscriber.channel.should be_kind_of AMQP::Channel }
    And  { subscriber.exchange.should be_kind_of AMQP::Exchange }
    And  { subscriber.exchange.type.should == :topic }
  end
end

