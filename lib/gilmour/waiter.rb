module Gilmour
  class Waiter
    def initialize
      @done = false
      @waiter_m = Mutex.new
      @waiter_c = ConditionVariable.new
    end

    def synchronize(&blk)
      @waiter_m.synchronize(&blk)
    end

    def signal
      synchronize do
        @done = true
        @waiter_c.signal
      end
    end

    def wait(timeout=nil)
      synchronize { @waiter_c.wait(@waiter_m, timeout) unless @done }
    end
  end
end
