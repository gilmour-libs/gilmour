class TestSubscriber < TestServiceBase
  ExclusiveTimeoutTopic = 'test.exclusive_topic'
  ExclusiveTimeoutReturn = 'test.exclusive_return'

  MultiTimeoutTopic = 'test.multi_timeout'
  MultiTimeoutReturn = 'test.multi_return'

  TimeoutTopic = "test.timeout"
  Topic = 'test.topic'
  WildcardTopic = 'test.wildcard.*'
  Simulation = 'simulate.topic'
  Republish = 'test.republish'
  GroupReturn = "group_return"
  GroupTopic = "test.group"
  ExclusiveTopic = "test.exclusive"
  ExitTopic = "topic.exit"
  ReListenTopic = "topic.relisten"


  def self.get_callback
    @callback
  end

  def self.callback
    @callback = Proc.new
  end

  2.times do |i|
    listen_to ExclusiveTopic, {exclusive: true} do
      publish(i, TestSubscriber::GroupReturn)
    end
  end

  2.times do
    listen_to GroupTopic do
      publish(request.body, TestSubscriber::GroupReturn)
      publish("2", TestSubscriber::GroupReturn)
    end
  end


  listen_to ExclusiveTimeoutTopic, {exclusive: true} do
    data, _, _ = Gilmour::Protocol.parse_response(request.body)
    logger.info "Will sleep for #{data} seconds now. But allowed timeout is 2."
    sleep data
    publish("2", TestSubscriber::ExclusiveTimeoutReturn)
  end

  3.times do
    listen_to MultiTimeoutTopic do
      data, _, _ = Gilmour::Protocol.parse_response(request.body)
      logger.info "Will sleep for #{data} seconds now. But allowed timeout is 2."
      sleep data
      publish("2", TestSubscriber::MultiTimeoutReturn)
    end
  end

  listen_to TimeoutTopic, {exclusive: false, timeout: 2} do
    data, _, _ = Gilmour::Protocol.parse_response(request.body)
    logger.info "Will sleep for #{data} seconds now. But allowed timeout is 2."
    sleep data
    respond 'Pong!'
  end

  listen_to ExitTopic do
    logger.info "Sleeping for 2 seconds, and then will exit"
    sleep 2
    exit!
  end

  listen_to ReListenTopic do
    # In forked environment this should not work.
    add_listener "test.world" do
      respond "Pong!"
    end
    respond "Pong"
  end

  listen_to Topic do
    if TestSubscriber.get_callback
      TestSubscriber.get_callback.call(request.topic, request.body)
    end
    respond 'Pong!' if request.body == 'Ping!'
  end

  listen_to WildcardTopic do
    if TestSubscriber.get_callback
      TestSubscriber.get_callback.call(request.topic, request.body)
    end
    respond request.body, 200
  end

  listen_to Simulation do |topic, data|
    @callback.call(topic, data)
  end

  listen_to Republish do
    resp = self
    publish(request.body, Topic) do |data, code|
      resp.respond data, 200, now: true
    end
  end
end
