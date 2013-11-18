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
    TestSubscriber.get_callback.call(request.topic, request.body)
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
    publish(request.body, 'test.topic')
  end
end
