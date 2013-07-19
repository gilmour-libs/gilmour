require 'rspec/given'
require 'securerandom'
require './testservice/test_service_base'

require_relative 'helpers/connection'

def install_test_subscriber(parent)
  waiter = Thread.new { sleep 1 while true }
  TestSubscriber.callback do |topic, data|
    @topic = topic
    @data = data
    waiter.kill
  end
  waiter
end

describe 'TestSubscriber' do
  Given(:subscriber) { TestServiceBase }
  Given { subscriber.load_subscriber('./testservice/subscribers/test_subscriber') }
  Then do
    handlers = subscriber.subscribers(TestSubscriber::Topic)
    module_present = handlers.find { |h| h[:subscriber] == TestSubscriber }
    module_present.should be_true
  end

  context 'Running Service' do
    before(:all) { @service = TestServiceBase.new(amqp_connection_options) }
    Given(:connection_opts) { amqp_connection_options }
    before(:all) { @service.start }
    context 'Handler Register' do
      When { install_test_subscriber(subscriber) }
      context 'Check registered handlers' do
        When(:handlers) do
          subscriber.subscribers(TestSubscriber::Simulation).map { |h| h[:handler]}
        end
        Then { handlers.each { |h| h.should be_kind_of Proc } }
        Then  do
          arg1 = TestSubscriber::Simulation
          handlers.each do |h|
            arg2 = SecureRandom.hex
            # simualate a handler call
            h.call(arg1, arg2)
            @topic.should == arg1
            @data.should == arg2
          end
        end
      end
    end

    context 'Recieve messages' do
      context 'Recieve a message' do
        Given(:ping_opts) { amqp_ping_options }
        When do
          @data = @topic = nil
          waiter = install_test_subscriber(TestServiceBase)
          publish_async(connection_opts,
                             ping_opts[:message],
                             'test.topic')
          waiter.join
        end
        Then do
          @data.should == ping_opts[:message]
          @topic.should == TestSubscriber::Topic
        end
      end

      context 'Recieve a message on a wildcard key' do
        Given(:wildcard_opts) { amqp_wildcard_options }
        When do
          @data = @topic = nil
          waiter = install_test_subscriber(TestServiceBase)
          publish_async(connection_opts,
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
        send_and_recv(connection_opts,
                      ping_opts[:message],
                      'test.topic')
      end
      Then do
        data, code = response
        data.should == ping_opts[:response]
        code.should == 200
      end
    end
  end
end
