# encoding: utf-8
require 'rspec/given'
require 'securerandom'
require './testservice/test_service_base'

require_relative 'helpers/connection'

def install_test_subscriber(parent)
  waiter = Thread.new { loop { sleep 1 } }
  TestSubscriber.callback do |topic, data|
    @topic = topic
    @data = data
    waiter.kill
  end
  waiter
end

describe 'TestSubscriber' do
  test_subscriber_path = './testservice/subscribers/test_subscriber'
  after(:all) do
    EM.stop
  end
  Given(:subscriber) { TestServiceBase }
  Given { subscriber.load_subscriber(test_subscriber_path) }
  Then do
    handlers = subscriber.subscribers(TestSubscriber::Topic)
    module_present = handlers.find { |h| h[:subscriber] == TestSubscriber }
    module_present.should be_true
  end

  context 'Running Service' do
    before(:all) do
      @service = TestServiceBase.new(redis_connection_options, 'redis')
    end
    Given(:connection_opts) { redis_connection_options }
    before(:all) do
      @service.registered_subscribers.each do |s|
        s.backend = 'redis'
      end
      @service.start
    end
    context 'Handler Register' do
      When { install_test_subscriber(subscriber) }
      context 'Check registered handlers' do
        When(:handlers) do
          subscriber.subscribers(TestSubscriber::Simulation)
          .map { |h| h[:handler] }
        end
        Then { handlers.each { |h| h.should be_kind_of Proc } }
        Then  do
          arg1 = TestSubscriber::Simulation
          handlers.each do |h|
            arg2 = SecureRandom.hex
            # simualate a handler call
            h.call(arg1, arg2)
            @topic.should be == arg1
            @data.should be == arg2
          end
        end
      end
    end

    context 'Receive messages' do
      context 'Receive a message' do
        Given(:ping_opts) { redis_ping_options }
        When do
          @data = @topic = nil
          waiter = install_test_subscriber(TestServiceBase)
          redis_publish_async(connection_opts,
                              ping_opts[:message],
                              'test.topic')
          waiter.join
        end
        Then do
          @data.should be == ping_opts[:message]
          @topic.should be == TestSubscriber::Topic
        end
        And { EM.reactor_running?.should be_true }
      end

      context 'Recieve a message on a wildcard key' do
        Given(:wildcard_opts) { redis_wildcard_options }
        When do
          @data = @topic = nil
          waiter = install_test_subscriber(TestServiceBase)
          redis_publish_async(connection_opts,
                              wildcard_opts[:message],
                              wildcard_opts[:topic])
          waiter.join
        end
        Then { @data.should == wildcard_opts[:message] }
        And  { @topic.should == wildcard_opts[:topic] }
      end
    end

    context 'Send and receive a message' do
      Given(:ping_opts) { amqp_ping_options }
      When(:response) do
        @data = @topic = nil
        redis_send_and_recv(connection_opts,
                            ping_opts[:message],
                            'test.topic')
      end
      Then do
        data, code = response
        data.should be == ping_opts[:response]
        code.should be == 200
      end
    end

    context 'Send message from subscriber' do
      Given(:ping_opts) { redis_ping_options }
      When do
        @data = @topic = nil
        waiter = install_test_subscriber(TestServiceBase)
        redis_publish_async(connection_opts,
                            ping_opts[:message],
                            'test.republish')
        waiter.join
      end
      Then do
        @data.should be == ping_opts[:message]
        @topic.should be == TestSubscriber::Topic
      end
      And { EM.reactor_running?.should be_true }
    end
  end
end
