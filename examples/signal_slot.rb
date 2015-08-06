# Run this as LOG_LEVEL=info ruby relay.rb
require 'gilmour'

class Server
  include Gilmour::Base
end

server = Server.new
gilmour = server.enable_backend('redis')

waiter = Gilmour::Waiter.new

waiter.add
gilmour.slot "user.added" do
  # This one will send out email notifications
  logger.info "Sent email notification for -"
  logger.info request.body
  waiter.done
end

waiter.add
gilmour.slot "user.added" do
  # This one will send out push notifications
  logger.info "Sent push notification for -"
  logger.info request.body
  waiter.done
end

EM.next_tick {
  gilmour.signal!({ username: 'foo', email: 'foo@bar.com' }, 'user.added')
}

waiter.wait


