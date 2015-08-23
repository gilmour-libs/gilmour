require 'em-hiredis'
require_relative 'backend'
require_relative '../waiter'

module Gilmour
  # Redis backend implementation
  class RedisBackend < Backend
    GilmourHealthKey = "gilmour.known_host.health"
    GilmourErrorBufferLen = 9999

    implements 'redis'

    attr_writer :report_errors
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

      waiter = Waiter.new

      Thread.new do
        EM.run do
          setup_pubsub(opts)
          waiter.signal
        end
      end

      waiter.wait

      @report_health = opts["health_check"] || opts[:health_check]
      @report_health = false if @report_health != true

      @report_errors = opts["broadcast_errors"] || opts[:broadcast_errors]
      @report_errors = true if @report_errors != false

      @ident = generate_ident
    end

    def ident #:nodoc:
      @ident
    end

    def generate_ident #:nodoc:
      "#{Socket.gethostname}-pid-#{Process.pid}-uuid-#{SecureRandom.uuid}"
    end

    def report_health? #:nodoc:
      @report_health
    end

    def report_errors? #:nodoc:
      @report_errors
    end

    def emit_error(message) #:nodoc:
      report = self.report_errors?

      if report == false
        Glogger.debug "Skipping because report_errors is false"
      elsif report == true
        publish_error message
      elsif report.is_a? String and !report.empty?
        queue_error report, message
      end
    end

    def setup_pubsub(opts) #:nodoc:
      @publisher = EM::Hiredis.connect(redis_host(opts))
      @subscriber = @publisher.pubsub_client
      register_handlers
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def register_handlers #:nodoc:
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

    def subscribe_topic(topic) #:nodoc:
      method = topic.index('*') ? :psubscribe : :subscribe
      @subscriber.method(method).call(topic)
    end

    def pmessage_handler(key, matched_topic, payload) #:nodoc:
      @subscriptions[key].each do |subscription|
        EM.defer(->{execute_handler(matched_topic, payload, subscription)})
      end
    end

    def register_response(sender, handler, timeout = 600) #:nodoc:
      topic = "gilmour.response.#{sender}"
      timer = EM::Timer.new(timeout) do # Simulate error response
        GLogger.info "Timeout: Killing handler for #{sender}"
        payload, _ = Gilmour::Protocol.create_request({}, 499)
        response_handler(topic, payload)
      end
      @response_handlers[topic] = {handler: handler, timer: timer}
      subscribe_topic(topic)
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def publish_error(messsage) #:nodoc:
      publish(messsage, Gilmour::ErrorChannel)
    end

    def queue_error(key, message) #:nodoc:
      @publisher.lpush(key, message) do
        @publisher.ltrim(key, 0, GilmourErrorBufferLen) do
          Glogger.debug "Error queued"
        end
      end
    end

    def acquire_ex_lock(sender) #:nodoc:
      @publisher.set(sender, sender, 'EX', 600, 'NX') do |val|
        EM.defer do
          yield val if val && block_given?
        end
      end
    end

    def response_handler(sender, payload)
      EM.defer { _response_handler(sender, payload) }
    end

    def _response_handler(sender, payload) #:nodoc:
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

    def send_response(sender, body, code) #:nodoc:
      publish(body, "gilmour.response.#{sender}", {}, code)
    end

    def get_subscribers #:nodoc:
      @subscriptions.keys
    end

    def reply_to(topic, opts={}, &blk) #:nodoc:
      if topic.index('*')
        raise ArgumentError.new("Subscribers cannot have wildcard topics")
      end
      super
    end

    def add_listener(topic, opts = {}, &blk) #:nodoc:
      # TODO: leave for backward compatibility
      opts[:exclusive] = opts[:excl] if opts[:exclusive].nil?
      if opts[:exclusive] && exclusive_group(opts).empty?
        raise ArgumentError.new("Invalid exclusive group")
      end
      opts[:handler] ||= blk
      @subscriptions[topic] ||= []
      @subscriptions[topic] << opts
      EM.next_tick { subscribe_topic(topic) }
    end

    def listeners(topic) #:nodoc:
      @subscriptions[topic] || []
    end

    def remove_listener(topic, handler = nil) #:nodoc:
      if handler
        subs = @subscriptions[topic]
        subs.delete_if { |e| e[:handler] == handler }
      else
        @subscriptions[topic] = []
      end
      @subscriber.unsubscribe(topic) if @subscriptions[topic].empty?
    end

    def send_message(sender, destination, payload, opts = {}, &blk) #:nodoc:
      timeout = opts[:timeout] || 600
      if opts[:confirm_subscriber]
        confirm_subscriber(destination) do |present|
          if !present
            blk.call(nil, 404) if blk
          else
            _send_message(sender, destination, payload, timeout, &blk)
          end
        end
      else
        _send_message(sender, destination, payload, timeout, &blk)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def _send_message(sender, destination, payload, timeout, &blk) #:nodoc:
      register_response(sender, blk, timeout) if block_given?
      @publisher.publish(destination, payload)
      sender
    end

    # Confirms whether an active subscriber is present. 
    # Params
    # +dest+:: The destination topic
    #
    # The given block is called with a true boolean
    # if active subscribers exist, else with false
    def confirm_subscriber(dest, &blk)
      @publisher.pubsub('numsub', dest) do |_, num|
        blk.call(num.to_i > 0)
      end
    rescue Exception => e
      GLogger.debug e.message
      GLogger.debug e.backtrace
    end

    def stop #:nodoc:
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
    def register_health_check #:nodoc:
      @publisher.hset GilmourHealthKey, self.ident, 'active'

      # - Start listening on a dyanmic topic that Health Monitor can publish
      # on.
      #
      # NOTE: Health checks are not run as forks, to ensure that event-machine's
      # ThreadPool has sufficient resources to handle new requests.
      #
      topic = "gilmour.health.#{self.ident}"
      backend = self
      add_listener(topic) do
        respond backend.get_subscribers
      end
    end

    def unregister_health_check #:nodoc:
      deleted = false

      @publisher.hdel(GilmourHealthKey, self.ident) do
        deleted = true
      end

      attempts = 0
      unless deleted || attempts > 5
        attempts += 1
        sleep 1
      end

    end

  end
end
