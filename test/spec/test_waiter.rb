# encoding: utf-8
require 'rspec/given'

require '../lib/gilmour/waiter'

describe 'TestAddGroup' do
  context "Wait Group" do
    it "Wait should return instantly" do
      wg = Gilmour::Waiter.new

      num = 2
      arr = []

      num.times do
        wg.add 1
        Thread.new {
          arr << "done"
          wg.done
        }
      end

      wg.wait
      expect(arr.length).to eq num
    end

    it "Wait should return after sleep" do
      wg = Gilmour::Waiter.new

      num = 2
      arr = []

      num.times do
        wg.add 1
        Thread.new {
          sleep(1)
          arr << true
          wg.done
        }
      end

      wg.wait
      expect(arr.length).to eq num
    end

    it "Wait should perform a yield" do
      wg = Gilmour::Waiter.new

      num = 2
      arr = []

      num.times do
        wg.add 1
        Thread.new {
          sleep(1)
          arr << true
          wg.done
        }
      end

      wg.wait do
        arr << true
      end

      expect(arr.length).to eq num+1
    end

    it "Wait should timeout" do
      wg = Gilmour::Waiter.new

      num = 2
      arr = []

      num.times do
        wg.add 1
        Thread.new {
          sleep(2)
          arr << true
          wg.done
        }
      end

      wg.wait(1) do
        arr << true
      end

      expect(arr.length).to eq 1
    end

    it "Done and Wait only" do
      wg = Gilmour::Waiter.new

      arr = []

      Thread.new {
        sleep(2)
        arr << true
        wg.done
      }

      wg.wait do
        arr << true
      end

      expect(arr.length).to eq 2
    end

    it "Signal should raise when using add" do
      wg = Gilmour::Waiter.new
      wg.add 1
      expect{wg.signal}.to raise_error(RuntimeError)
    end

  end
end
