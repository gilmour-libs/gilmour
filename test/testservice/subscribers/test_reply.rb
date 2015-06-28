class TestReplier < TestServiceBase
  TimeoutTopic = "test.reply.timeout"
  Topic = 'test.reply.topic'
  WildcardTopic = 'test.reply.wildcard.*'
  Simulation = 'simulate.reply.topic'
  Republish = 'test.reply.republish'
  GroupReturn = "reply.group_return"
  GroupTopic = "test.reply.group"
  ExclusiveTopic = "test.reply.exclusive"
  ExitTopic = "topic.reply.exit"
  ReListenTopic = "topic.reply.relisten"


  def self.get_callback
    @callback
  end

  def self.callback
    @callback = Proc.new
  end

  2.times do |i|
    reply_to ExclusiveTopic do
      signal!(i, TestReplier::GroupReturn)
    end
  end

  2.times do
    reply_to GroupTopic do
      signal!(request.body, TestReplier::GroupReturn)
      signal!("2", TestReplier::GroupReturn)
    end
  end

  reply_to TimeoutTopic, {timeout: 2} do
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

  reply_to ReListenTopic do
    # In forked environment this should not work.
    reply_to "test.world", excl_group: 'relisten' do
      respond "Pong!"
    end
    respond "Pong"
  end

  reply_to Topic do
    if TestReplier.get_callback
      $stderr.puts 'Calling callback from handler'
      TestReplier.get_callback.call(request.topic, request.body)
    end
    respond 'Pong!' if request.body == 'Ping!'
  end

  reply_to Simulation do |topic, data|
    @callback.call(topic, data)
  end

  reply_to Republish do
    resp = self
    request!(request.body, Topic) do |data, code|
      resp.respond data, 200, now: true
    end
    delay_response
  end
end
