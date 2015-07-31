module Gilmour
  class Waiter
    def initialize
      @count = 0
      @done = false
      @waiter_m = Mutex.new
      @waiter_c = ConditionVariable.new
    end

    def add n = 1
      synchronize { @count += n }
    end

    def done
      synchronize do
        @count -= n
        if @count == 0
          signal
        end
      end
    end

    def synchronize(&blk)
      @waiter_m.synchronize(&blk)
    end

    def signal
      synchronize do
        @done = true
        @count = 0
        @waiter_c.signal
      end
    end

    def wait(timeout=nil)
      synchronize { @waiter_c.wait(@waiter_m, timeout) unless @done }
      yield if block_given?
    end
  end
end

def test
  wg = Gilmour::Waiter.new
  wg.add 3

  3.times do
    Thread.new {
      t = rand(10000) / 10000.0
      sleep(t)
      puts "done\n"
      wg.done
    }
  end

  wg.wait do
    puts "All jobs done"
  end

end
