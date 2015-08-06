class EchoSubscriber < EventServer
  reply_to 'echo', excl_group: 'echo' do
    if request.body == 'quit'
      respond nil
    else
      $stderr.puts request.body
      respond "#{request.topic}"
    end
  end

  reply_to 'forkecho', fork:true, excl_group: 'forkecho' do
    if request.body == 'quit'
      respond nil
    else
      $stderr.puts "Forked response. pid: #{Process.pid}"
      respond "#{request.topic}"
    end
  end

end


