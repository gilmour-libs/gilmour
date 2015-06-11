class TestSubscriber < TestServiceBase
  Topic = 'test.topic'
  WildcardTopic = 'test.wildcard.*'
  Simulation = 'simulate.topic'
  Republish = 'test.republish'
  GroupReturn = "group_return"
  GroupTopic = "test.group"
  ExclusiveTopic = "test.exclusive"


  def self.get_callback
    @callback
  end

  def self.callback
    @callback = Proc.new
  end

  2.times do |i|
    listen_to ExclusiveTopic, true do
      publish(i, TestSubscriber::GroupReturn)
    end
  end

  2.times do
    listen_to GroupTopic do
      publish(request.body, TestSubscriber::GroupReturn)
    end
  end

  listen_to Topic do
    if TestSubscriber.get_callback
      TestSubscriber.get_callback.call(request.topic, request.body)
    end
    respond 'Pong!' if request.body == 'Ping!'
  end

  listen_to WildcardTopic do
    TestSubscriber.get_callback.call(request.topic, request.body)
    nil
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
