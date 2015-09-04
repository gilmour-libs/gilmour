# encoding: utf-8

require 'rspec/given'
require 'redis'

require_relative 'helpers/connection'
require './testservice/test_service_base'

describe TestServiceBase do
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

  context 'Connect to Redis' do
    Given(:subscriber) do
      TestServiceBase.new(redis_connection_options, 'redis')
    end
    Given(:backend) { subscriber.backends['redis'] }
    Then { backend.subscriber.should be_kind_of EM::Hiredis::PubsubClient }
    And  { backend.publisher.should be_kind_of EM::Hiredis::Client }
  end
end
