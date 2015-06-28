# encoding: utf-8
require 'rspec/given'
require 'securerandom'
require './testservice/test_service_base'

require_relative 'helpers/common'
require_relative 'helpers/connection'

def install_test_subscriber(parent)
  waiter = Waiter.new
  TestReplier.callback do |topic, data|
    @topic = topic
    @data = data
    $stderr.puts "Got callback #{topic}, #{data}"
    waiter.signal
  end
  waiter
end

describe 'TestReplier' do
  opts = redis_connection_options
  opts[:health_check] = true

  test_subscriber_path = './testservice/subscribers/test_reply'
  after(:all) do
    EM.stop
  end
  Given(:subscriber) { TestServiceBase }
  Given do
    subscriber.load_subscriber(test_subscriber_path)
  end
  Then do
    handlers = subscriber.subscribers(TestReplier::Topic)
    module_present = handlers.find { |h| h[:subscriber] == TestReplier }
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

    context 'Handler Register' do
      When { install_test_subscriber(subscriber) }
      context 'Check registered handlers' do
        When(:handlers) do
          subscriber.subscribers(TestReplier::Simulation)
          .map { |h| h[:handler] }
        end
        Then { handlers.each { |h| h.should be_kind_of Proc } }
        Then  do
          arg1 = TestReplier::Simulation
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

    context 'Non Fork can add dynamic listeners', :fork_dynamic do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:code) do
        waiter = Waiter.new
        dynamicaly_subscribed = 0

        sub.request!(1, TestReplier::ReListenTopic) do |d, c|
          sub.confirm_subscriber(sub.request_destination("test.world")) do |present|
            dynamicaly_subscribed = present
            waiter.signal
          end
        end

        waiter.wait(5)
        dynamicaly_subscribed
      end
      Then do
        code.should be == true
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

        sub.request!(3, TestReplier::TimeoutTopic, {:timeout => 1}) do |d, c|
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

    context 'Handler sleeps longer than the Timeout' do
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

        backend = @service.get_backend("redis")
        backend.broadcast_errors = true
        sub.slot Gilmour::ErrorChannel do
          waiter_error.signal
        end

        sub.request!(3, TestReplier::TimeoutTopic) do |d, c|
          code = c
          waiter_code.signal
        end

        waiter_code.wait(5)
        waiter_error.wait(5)

        backend.broadcast_errors = false
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

        sub.request!(1, TestReplier::TimeoutTopic) do |d, c|
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

    context 'Receive messages' do
      context 'Receive a message' do
        Given(:ping_opts) { redis_ping_options }
        When do
          @data = @topic = nil
          waiter = install_test_subscriber(TestServiceBase)
          redis_publish_async(connection_opts,
                              ping_opts[:message],
                              "{gilmour.request.#{TestReplier::Topic}")
          $stderr.puts "Waiting for callback"
          waiter.wait(5)
          $stderr.puts "After callback - #{@data}, #{@topic}"
        end
        Then do
          @data.should be == ping_opts[:message]
          @topic.should be == TestReplier::Topic
        end
        And { expect(EM.reactor_thread.alive?).to be_truthy }
      end
    end

    context 'Publish sans subscriber timeout' do
      Given(:ping_opts) do
        redis_ping_options
      end

      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When(:response) do
        waiter = Waiter.new
        data = code = nil
        sub.request!(ping_opts[:message], "hello.world") do |d, c|
          data = d
          code = c
          waiter.signal
        end
        waiter.wait(5)
        [data, code]
      end
      Then do
        data, code = response
        data.should be == nil
        code.should be == nil
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
        sub.request!(ping_opts[:message], TestReplier::Topic) do |d, c|
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

    context 'Send once, Receive twice' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        waiter = Waiter.new

        actual_ret = []

        group_proc = sub.slot TestReplier::GroupReturn do
          actual_ret.push(request.body)
          waiter.signal if actual_ret.length == 4
        end

        sub.signal!(ping_opts[:message], TestReplier::GroupTopic)
        waiter.wait(5)

        sub.remove_slot TestReplier::GroupReturn, group_proc
        actual_ret
      end
      Then do
        response.select { |e| e == ping_opts[:message] }.size.should == 2
        response.select { |e| e == "2" }.size.should == 2
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

        group_proc = sub.add_listener TestReplier::GroupReturn do
          actual_ret.push(request.body)
          waiter.signal if actual_ret.length == 1
        end

        sub.request!(ping_opts[:message], TestReplier::ExclusiveTopic)

        waiter.wait(5)
        sub.remove_listener TestReplier::GroupReturn, group_proc
        actual_ret
      end
      Then do
        response.size.should == 1
      end
    end

    context 'Send message from subscriber' do
      Given(:ping_opts) { redis_ping_options }
      When(:sub) do
        Gilmour::RedisBackend.new({})
      end
      When (:response) do
        data = code = nil
        waiter = Waiter.new
        sub.request!(ping_opts[:message], 'test.republish') do |d, c|
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
      And { expect(EM.reactor_thread.alive?).to be_truthy }
    end
  end
end
