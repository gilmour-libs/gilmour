require 'gilmour'

class Server
  include Gilmour::Base
end
server = Server.new
gilmour = server.enable_backend('redis')

gilmour.reply_to 'one' do
  respond request.body.merge({'one' => 'foo'})
end

gilmour.reply_to 'two' do
  respond request.body.merge({'two' => 'foo'})
end

gilmour.reply_to 'badtwo' do
  respond request.body.merge({'badtwo' => 'foo'}), 500
end

gilmour.reply_to 'three' do
  respond request.body.merge({'three' => 'foo'})
end

sleep 1

composed = gilmour.compose([{topic: 'one'}, {topic: 'two'},
                            {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
compose_waiter = Gilmour::Waiter.new
composed.execute({zero: 'foo'}) do |data, code|
  puts "composition:"
  puts data # data will have the keys 'one', 'two', 'three' and 'anotherthree'
  compose_waiter.signal
end
compose_waiter.wait

andand = gilmour.andand([{topic: 'one'}, {topic: 'two'},
                         {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
andand_waiter = Gilmour::Waiter.new
andand.execute do |data, code|
  puts "\nandand:"
  puts data # data will have the keys 'three' and 'anotherthree'
  andand_waiter.signal
end
andand_waiter.wait


error_andand = gilmour.andand([{topic: 'one'}, {topic: 'badtwo'},
                               {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
error_andand_waiter = Gilmour::Waiter.new
continuation = nil
error_andand.execute do |data, code, c|
  continuation = c # the part of the pipeline that was not executed
  puts "\nerror_andand:\n code - #{code}" # should be 500 fron badtwo
  puts "error_andand:\n data - #{data}" # this will have keys one and badtwo
  error_andand_waiter.signal
end
error_andand_waiter.wait

continuation_waiter = Gilmour::Waiter.new
continuation.execute do |data, code|
  puts "\ncontinuation:"
  puts data # This will have keys three and anotherthree
  continuation_waiter.signal
end
continuation_waiter.wait


batch = gilmour.batch([{topic: 'one'}, {topic: 'badtwo'},
                       {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
batch_waiter = Gilmour::Waiter.new
batch.execute do |data, code|
  puts "\nbatch:"
  puts data # this will have keys three and anotherthree (wont abort on badtwo)
  batch_waiter.signal
end
batch_waiter.wait

batch_record =  gilmour.batch([{topic: 'one'}, {topic: 'badtwo'},
                               {topic: 'three', message: {'anotherthree' => 'anotherthree'}}],
true) # true will record all responses in an array
batch_record_waiter = Gilmour::Waiter.new
batch_record.execute do |data, code|
  puts "\nbatch with record:"
  puts data # This is an array
  batch_record_waiter.signal
end
batch_record_waiter.wait

parallel = gilmour.parallel([{topic: 'one'}, {topic: 'badtwo'}])
parallel.execute do |data, code|
  puts "\nparallel:"
  puts code
  data.each { |d| puts d }
end

t_andand = gilmour.andand([{topic: 'one'}, {topic: 'two'}])
# Use the above composition inside another
compose = gilmour.compose([t_andand, {topic: 'three'}])
combination_waiter = Gilmour::Waiter.new
compose.execute do |data, code|
  puts "\nnested composition"
  puts data # will have keys two and three
  combination_waiter.signal
end
combination_waiter.wait

