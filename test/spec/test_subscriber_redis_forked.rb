# encoding: utf-8
require 'rspec/given'
require 'securerandom'
require './testservice/test_service_base'

require_relative 'helpers/connection'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end

describe 'TestSubscriberFork' do
  opts = redis_connection_options
  opts["multi_process"] = true

  test_subscriber_path = './testservice/subscribers/test_subscriber'
  after(:all) do
    EM.stop if EM.reactor_running?
  end

  Given(:subscriber) { TestServiceBase }
  Given {
    subscriber.load_subscriber(test_subscriber_path)
  }
  Then do
    handlers = subscriber.subscribers(TestSubscriber::Topic)
    module_present = handlers.find { |h| h[:subscriber] == TestSubscriber }
    module_present.should be_truthy
  end

  context 'Running Service' do
    before(:all) do
      @service = TestServiceBase.new(opts, 'redis')
    end
    Given(:connection_opts) { redis_connection_options }
    before(:all) do
      @service.registered_subscribers.each do |s|
        s.backend = 'redis'
      end
      @service.start
    end

    context 'Send and receive a message' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:response) do
        waiter = Thread.new { loop { sleep 1 } }
        data = code = nil
        sub.publish(ping_opts[:message], TestSubscriber::Topic, { confirm_subscriber: true }) do |d, c|
          data = d
          code = c
          waiter.kill
        end
        waiter.join
        [data, code]
      end
      Then do
        data, code = response
        data.should be == ping_opts[:response]
        code.should be == 200
      end
    end

    context 'Send & Recieve a message on a wildcard key' do
      Given(:wildcard_opts) { redis_wildcard_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:response) do
        data = code = nil

        waiter = Thread.new { loop { sleep 1 } }

        sub.publish(wildcard_opts[:message], wildcard_opts[:topic]) do |d,c|
          data = d
          code = 200
          waiter.kill
        end

        waiter.join
        [data, code]
      end
      Then do
        data, code = response
        data.should be == wildcard_opts[:message]
        code.should be == 200
      end
    end

    context 'Send once, Receive twice' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        waiter = Thread.new { loop { sleep 1 } }

        actual_ret = []

        sub.add_listener TestSubscriber::GroupReturn do
          actual_ret.push(request.body)
        end

        sub.publish(ping_opts[:message], TestSubscriber::GroupTopic)

        waiter.join(5)
        actual_ret
      end
      Then do
        expected = [ping_opts[:message], "2"]
        response.should be == expected + expected
      end
    end

    context 'Send once, Receive Once' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        waiter = Thread.new { loop { sleep 1 } }

        actual_ret = []

        sub.add_listener TestSubscriber::GroupReturn do
          actual_ret.push(request.body)
        end

        sub.publish(ping_opts[:message], TestSubscriber::ExclusiveTopic)

        waiter.join(5)
        actual_ret
      end
      Then do
        response.should be == [0]
      end
    end

    context 'Send message from subscriber' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      Then do
        waiter = Thread.new { loop { sleep 1 } }
        code = nil
        sub.publish(ping_opts[:message], TestSubscriber::Republish) do |d, c|
          code = c
          waiter.kill
        end
        waiter.join()
        expect(code).to eq(500)
      end
    end

    context 'Error in child registration' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        @service.get_backend("redis")
      end
      Then do
        expect {
          sub.add_listener TestSubscriber::Topic do |t, d|
            puts t, d
          end
        }.to raise_error
      end
    end

  end
end
