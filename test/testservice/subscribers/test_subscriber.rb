class TestSubscriber < TestServiceBase
  Topic = 'test.topic'
  WildcardTopic = 'test.wildcard.*'
  Simulation = 'simulate.topic'
  Republish = 'test.republish'

  def self.get_callback
    @callback
  end

  def self.callback
    @callback = Proc.new
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
