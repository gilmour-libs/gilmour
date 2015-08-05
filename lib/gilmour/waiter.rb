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
        @count -= 1
        if @count <= 0
          @done = true
          @waiter_c.broadcast
        end
      end
    end

    def synchronize(&blk)
      @waiter_m.synchronize(&blk)
    end

    def signal
      if @count != 0
        raise 'Cannot use signal alongside add/done'
      end

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
