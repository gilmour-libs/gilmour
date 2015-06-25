# encoding: utf-8
require 'rspec/given'
require 'securerandom'
require './testservice/test_service_base'

require_relative 'helpers/common'
require_relative 'helpers/connection'

describe 'TestSubscriberFork' do
  opts = redis_connection_options
  opts['health_check'] = false

  test_subscriber_path = './testservice/subscribers/test_subscriber'
  after(:all) do
    EM.stop if EM.reactor_running?
  end

  Given(:subscriber) { TestServiceBase }
  Given do
    subscriber.load_subscriber(test_subscriber_path)
    subscriber.subscribers.each do |topic, arr|
      arr.each do |s|
        s[:fork] = true
      end
    end
  end
  Then do
    handlers = subscriber.subscribers(TestSubscriber::Topic)
    module_present = handlers.find { |h| h[:subscriber] == TestSubscriber }
    module_present.should be_truthy
  end

  context 'Running Service' do
    before(:all) do
      @service = TestServiceBase.new(opts, 'redis')
    end
    Given(:connection_opts) { opts }
    before(:all) do
      @service.registered_subscribers.each do |s|
        s.backend = 'redis'
      end

      @service.start
    end

    context 'Fork not allowed to add dynamic listeners', :fork_dynamic do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        waiter = Waiter.new
        dynamicaly_subscribed = 0

        sub.publish(1, TestSubscriber::ReListenTopic) do |d, c|
          sub.confirm_subscriber("test.world") do |present|
            dynamicaly_subscribed = present
            waiter.signal
          end
        end

        waiter.wait(5)
        dynamicaly_subscribed
      end
      Then do
        code.should be == false
      end
    end

    context 'Publisher side timeout' do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        code = nil
        waiter_code = Waiter.new

        sub.publish(3, TestSubscriber::TimeoutTopic, {:timeout => 1}) do |d, c|
          code = c
          waiter_code.signal
        end

        waiter_code.wait(5)
        code
      end
      Then do
        code.should be == 499
      end
    end

    context 'Handler to Test exits' do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        waiter_error = Waiter.new
        waiter_code = Waiter.new
        code = nil

        error_listener_proc = sub.add_listener Gilmour::ErrorChannel do |d, c|
          waiter_error.signal
        end

        sub.publish(1, TestSubscriber::ExitTopic) do |d, c|
          code = c
          waiter_code.signal
        end

        waiter_code.wait(5)
        waiter_error.wait(5)

        sub.remove_listener Gilmour::ErrorChannel, error_listener_proc
        code
      end
      Then do
        code.should be == 500
      end
    end

    context 'Handler to Test exits with LPush' do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        code = nil

        error_key = "hello.world"
        backend = @service.get_backend("redis")
        backend.broadcast_errors = error_key

        sub.publish(1, TestSubscriber::ExitTopic)

        th = Thread.new{
          5.times {
            sleep 1
            sub.publisher.lpop(error_key) do |val|
              if val.is_a? String
                code = 500
                th.kill
              end
            end
          }
        }

        th.join
        backend.broadcast_errors = true
        code
      end
      Then do
        code.should be == 500
      end
    end

    context 'Handler sleeps longer than the Timeout' do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        waiter = Waiter.new
        code = nil

        sub.publish(3, TestSubscriber::TimeoutTopic) do |d, c|
          code = c
          waiter.signal
        end

        waiter.wait(5)
        code
      end
      Then do
        code.should be == 504
      end
    end

    context 'Handler sleeps just enough to survive the timeout' do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        waiter = Waiter.new
        code = nil

        sub.publish(1, TestSubscriber::TimeoutTopic) do |d, c|
          code = c
          waiter.signal
        end

        waiter.wait(5)
        code
      end
      Then do
        code.should be == 200
      end
    end

    context 'Send and receive a message' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:response) do
        waiter = Waiter.new
        data = code = nil
        sub.publish(ping_opts[:message], TestSubscriber::Topic, { confirm_subscriber: true }) do |d, c|
          data = d
          code = c
          waiter.signal
        end
        waiter.wait(5)
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

        waiter = Waiter.new

        sub.publish(wildcard_opts[:message], wildcard_opts[:topic]) do |d,c|
          data = d
          code = 200
          waiter.signal
        end

        waiter.wait(5)
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
        waiter = Waiter.new

        actual_ret = []

        sub.add_listener TestSubscriber::GroupReturn do
          actual_ret.push(request.body)
        end

        sub.publish(ping_opts[:message], TestSubscriber::GroupTopic)

        waiter.wait(5)
        actual_ret
      end
      Then do
        response.select { |e| e == ping_opts[:message] }.size.should == 2
        response.select { |e| e == "2" }.size.should == 2
      end
    end

    context 'Check Exclusive Parallelism' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        waiter = Waiter.new

        actual_ret = []

        sub.add_listener TestSubscriber::ExclusiveTimeoutReturn do
          actual_ret.push(request.body)
        end

        3.times do
          sub.publish(3, TestSubscriber::ExclusiveTimeoutTopic)
        end

        waiter.wait(5)
        actual_ret
      end
      Then do
        response.select { |e| e == "2" }.size.should == 3
      end
    end

    context 'Check Parallelism' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        waiter = Waiter.new

        actual_ret = []

        sub.add_listener TestSubscriber::MultiTimeoutReturn do
          actual_ret.push(request.body)
        end

        sub.publish(3, TestSubscriber::MultiTimeoutTopic)

        waiter.wait(5)
        actual_ret
      end
      Then do
        response.select { |e| e == "2" }.size.should == 3
      end
    end

    context 'Send once, Receive Once' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        waiter = Waiter.new

        actual_ret = []

        sub.add_listener TestSubscriber::GroupReturn do
          actual_ret.push(request.body)
        end

        sub.publish(ping_opts[:message], TestSubscriber::ExclusiveTopic)

        waiter.wait(5)
        actual_ret
      end
      Then do
        response.size.should == 1
      end
    end
  end
end
