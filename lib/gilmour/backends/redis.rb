require 'em-hiredis'
require_relative 'backend'

module Gilmour
  # Redis backend implementation
  class RedisBackend < Backend
    RedisHealthKey = "known_hosts.health"
    RedisTopicsKey = "known_hosts.topics"

    implements 'redis'

    attr_reader :subscriber
    attr_reader :publisher

    def redis_host(opts)
      host = opts[:host] || '127.0.0.1'
      port = opts[:port] || 6379
      db = opts[:db] || 0
      "redis://#{host}:#{port}/#{db}"
    end

    def initialize(opts)
      @response_handlers = {}
      @subscriptions = {}
      done = false
      wait_m = Mutex.new
      wait_c = ConditionVariable.new
      Thread.new do
        EM.run do
          setup_pubsub(opts)
          wait_m.synchronize {
            done = true
            wait_c.signal
          }
        end
      end
      wait_m.synchronize {
        wait_c.wait(wait_m) unless done
      }
      super
    end

    def setup_pubsub(opts)
      @publisher = EM::Hiredis.connect(redis_host(opts))
      @subscriber = @publisher.pubsub_client
      register_handlers
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def register_handlers
      @subscriber.on(:pmessage) do |key, topic, payload|
        pmessage_handler(key, topic, payload)
      end
      @subscriber.on(:message) do |topic, payload|
        begin
        if topic.start_with? 'gilmour.response.'
          response_handler(topic, payload)
        else
          pmessage_handler(topic, topic, payload)
        end
        rescue Exception => e
          GLogger.debug e.message
          GLogger.debug e.backtrace
        end
      end
    end

    def subscribe_topic(topic)
      method = topic.index('*') ? :psubscribe : :subscribe
      @subscriber.method(method).call(topic)
    end

    def pmessage_handler(key, matched_topic, payload)
      @subscriptions[key].each do |subscription|
        EM.defer(->{execute_handler(matched_topic, payload, subscription)})
      end
    end

    def register_response(sender, handler, timeout = 600)
      topic = "gilmour.response.#{sender}"
      timer = EM::Timer.new(timeout) do # Simulate error response
        GLogger.info "Timeout: Killing handler for #{sender}"
        payload, _ = Gilmour::Protocol.create_request({}, 504)
        response_handler(topic, payload)
      end
      @response_handlers[topic] = {handler: handler, timer: timer}
      subscribe_topic(topic)
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def acquire_ex_lock(sender)
      @publisher.set(sender, sender, 'EX', 600, 'NX') do |val|
        yield val if val && block_given?
      end
    end

    def response_handler(sender, payload)
      data, code, _ = Gilmour::Protocol.parse_response(payload)
      handler = @response_handlers.delete(sender)
      @subscriber.unsubscribe(sender)
      if handler
        handler[:timer].cancel
        handler[:handler].call(data, code)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def send_response(sender, body, code)
      publish(body, "gilmour.response.#{sender}", {}, code)
    end

    def setup_subscribers(subs = {})
      @subscriptions.merge!(subs)
      EM.defer do
        subs.keys.each { |topic| subscribe_topic(topic) }
      end
    end

    def add_listener(topic, &handler)
      @subscriptions[topic] ||= []
      @subscriptions[topic] << { handler: handler }
      subscribe_topic(topic)
    end

    def remove_listener(topic, handler = nil)
      if handler
        subs = @subscriptions[topic]
        subs.delete_if { |e| e[:handler] == handler }
      else
        @subscriptions[topic] = []
      end
      @subscriber.unsubscribe(topic) if @subscriptions[topic].empty?
    end

    def send(sender, destination, payload, opts = {}, &blk)
      timeout = opts[:timeout] || 600
      if opts[:confirm_subscriber]
        confirm_subscriber(destination) do |present|
          if !present
            blk.call(nil, 404) if blk
          else
            _send(sender, destination, payload, timeout, &blk)
          end
        end
      else
        _send(sender, destination, payload, timeout, &blk)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def _send(sender, destination, payload, timeout, &blk)
      register_response(sender, blk, timeout) if block_given?
      @publisher.publish(destination, payload)
      sender
    end

    def confirm_subscriber(dest, &blk)
      @publisher.pubsub('numsub', dest) do |_, num|
        blk.call(num.to_i > 0)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def stop
      @subscriber.close_connection
    end

    # TODO: Health checks currently use Redis to keep keys in a data structure.
    # An alternate approach would be that monitor subscribes to a topic
    # and records nodenames that request to be monitored. The publish method
    # should fail if there is no definite health monitor listening. However,
    # that would require the health node to be running at all points of time
    # before a Gilmour server starts up. To circumvent this dependency, till
    # monitor is stable enough, use Redis to save/share these data structures.
    #
    def register_health_check
      @publisher.hset RedisHealthKey, self.ident, 'active'

      # - Start listening on a dyanmic topic that Health Monitor can publish
      # on.
      #
      # NOTE: The subscriber is responsible for returning data within 5
      # seconds, to be considered as healthy, else the node is marked as
      # unhealthy. 5 consecutive health check failures are marked as permanent
      # failure and an event is raised for system administrators.
      #
      # NOTE: Health checks are not run as forks, to ensure that event-machine's
      # ThreadPool has sufficient resources to handle new requests.
      #
      topic = "gilmour.health.#{self.ident}"
      add_listener(topic) do
        respond "Pong"
      end

      # TODO: Need to do these manually. Alternate is to return the handler
      # hash from add_listener.
      @subscriptions[topic][0][:exclusive] = true
      @subscriptions[topic][0][:timeout] = 5

    end

    def register_essential_topics
      seperator = "::"
      topics = self.essential_topics.join(seperator)

      @publisher.hset RedisTopicsKey, self.ident, topics
    end

  end
end
