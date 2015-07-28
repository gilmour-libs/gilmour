# StringIO also do as IO, but IO#reopen fails.
# The problem is that a StringIO cannot exist in the O/S's file descriptor
# table. STDERR.reopen(...) at the low level does a dup() or dup2() to
# copy one file descriptor to another.
#
# I have two options:
#
# (1) $stderr = StringIO.new
# Then any program which writes to $stderr will be fine. But anything
# which writes to STDERR will still go to file descriptor 2.
#
# (2) reopen STDERR with something which exists in the O/S file descriptor
# table: e.g. a file or a pipe.
#
# I canot use a file, hence a Pipe.

def capture_output(pipes, capture_stdout=false)
  streams = []

  if capture_stdout == true
    streams << $stdout
  end

  streams << $stderr

  # Save the streams to be reassigned later.
  # Actually it doesn't matter because the child process would be killed
  # anyway after the work is done.
  saved = streams.each do |stream|
    stream.dup
  end

  begin
    streams.each_with_index do |stream, ix|
      # Probably I should not use IX, otherwise stdout and stderr can arrive
      # out of order, which they should?
      # If I reopen both of them on the same PIPE, they are guaranteed to
      # arrive in order.
      stream.reopen(pipes[ix])
    end
    yield
  ensure
    # This is sort of meaningless, just makes sense aesthetically.
    # To return what was borrowed.
    streams.each_with_index do |stream, i|
      stream.reopen(saved[i])
    end
  end
end
