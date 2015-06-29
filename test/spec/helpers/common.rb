
class Waiter
  def initialize
    @waiter_m = Mutex.new
    @waiter_c = ConditionVariable.new
  end

  def synchronize(&blk)
    @waiter_m.synchronize(&blk)
  end

  def signal
    synchronize { @waiter_c.signal }
  end

  def wait(timeout=nil)
    synchronize { @waiter_c.wait(@waiter_m, timeout) }
  end
end

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end
