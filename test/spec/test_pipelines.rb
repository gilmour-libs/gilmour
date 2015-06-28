require 'rspec/given'

require_relative '../../lib/gilmour'
require_relative 'helpers/common'

class Server
  include Gilmour::Base
end

describe 'Pipelines' do
  after(:all) do
    EM.stop
  end
  Given(:gilmour) { 
    Server.new.enable_backend('redis')
  }
  When {
    gilmour.reply_to 'one' do
      respond request.body.merge({'one' => 'foo'})
    end
  }
  When {
    gilmour.reply_to 'two' do
      respond request.body.merge({'two' => 'foo'})
    end
  }
  When {
    gilmour.reply_to 'badtwo' do
      respond request.body.merge({'badtwo' => 'foo'}), 500
    end
  }
  When {
    gilmour.reply_to 'three' do
      respond request.body.merge({'three' => 'foo'})
    end
  }
  When {
    # Make sure all subscribers are active
    sleep 1
  }
  context 'Compose' do
    Given(:composed) {
      gilmour.compose([{topic: 'one'}, {topic: 'two'},
                       {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
    }
   Then {
      waiter = Waiter.new
      code = data = nil
      composed.execute({zero: 'foo'}) do |d, c|
        data = d
        code = c
        waiter.signal
      end
      waiter.wait
      ['zero', 'one', 'two', 'three', 'anotherthree'].each do |k|
        expect(data.has_key?(k)).to be_truthy
      end
      expect(code).to equal(200)
    }
  end

  context 'AndAnd' do
    context 'All successful' do
      Given(:andand) {
        gilmour.andand([{topic: 'one'}, {topic: 'two'},
                        {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
      }
      Then {
        waiter = Waiter.new
        code = data = nil
        andand.execute do |d, c|
          data = d
          code = c
          waiter.signal
        end
        waiter.wait
        expect(data.keys.size).to equal(2)
        ['three', 'anotherthree'].each do |k|
          expect(data.has_key?(k)).to be_truthy
        end
        expect(code).to equal(200)
      }
    end

    context 'Interrupted' do
      Given(:interupted) {
        gilmour.andand([{topic: 'one'}, {topic: 'badtwo'},
                        {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
      }
      Given(:firsttry) {
        waiter = Waiter.new
        cont = code = data = nil
        interupted.execute do |d, c, continuation|
          data = d
          code = c
          cont = continuation
          waiter.signal
        end
        waiter.wait
        {code: code, data: data, continuation: cont}
      }
      Given(:secondtry) {
        waiter = Waiter.new
        code = data = nil
        firsttry[:continuation].execute do |d, c|
          data = d
          code = c
          waiter.signal
        end
        waiter.wait
        {code: code, data: data}
      }
      Then {
        data = firsttry[:data]
        code = firsttry[:code]
        expect(data.keys.size).to equal(1)
        ['badtwo'].each do |k|
          expect(data.has_key?(k)).to be_truthy
        end
        expect(code).to equal(500)
      }
      And {
        data = secondtry[:data]
        code = secondtry[:code]
        expect(data.keys.size).to equal(2)
        ['three', 'anotherthree'].each do |k|
          expect(data.has_key?(k)).to be_truthy
        end
        expect(code).to equal(200)
     }
    end
  end

  context 'Batch' do
    context 'Without record' do
      Given(:batch) {
        gilmour.batch([{topic: 'one'}, {topic: 'badtwo'},
                        {topic: 'three', message: {'anotherthree' => 'anotherthree'}}])
      }
      Then {
        waiter = Waiter.new
        code = data = nil
        batch.execute do |d, c|
          data = d
          code = c
          waiter.signal
        end
        waiter.wait
        expect(data).to be_kind_of(Hash)
        expect(data.keys.size).to equal(2)
        ['three', 'anotherthree'].each do |k|
          expect(data.has_key?(k)).to be_truthy
        end
        expect(code).to equal(200)
      }
    end

    context 'With record' do
      Given(:batchrecord) {
        gilmour.batch([{topic: 'one'}, {topic: 'badtwo'},
                        {topic: 'three', message: {'anotherthree' => 'anotherthree'}}],
                      true)
      }
      Then {
        waiter = Waiter.new
        code = data = nil
        batchrecord.execute do |d, c|
          data = d
          code = c
          waiter.signal
        end
        waiter.wait
        expect(data).to be_kind_of(Array)
        expect(data.size).to equal(3)
        %w{one badtwo three anotherthree}.each do |k|
          expect(data.find {|o| o[:data].has_key?(k)}).not_to be_nil
        end
      }
    end
  end
end

