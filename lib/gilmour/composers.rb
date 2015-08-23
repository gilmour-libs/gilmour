
module Gilmour
  module Composers
    class Request #:nodoc:
      attr_reader :topic
      def initialize(backend, spec)
        @backend = backend
        @spec = spec
        if spec.kind_of?(Hash)
          @message = spec[:message] || spec['message'] || {}
          @topic = spec[:topic] || spec['topic']
          if !@topic
            raise ArgumentError.new("Request topic cannot be empty in a request spec")
          end
          @opts = spec[:opts] || spec['opts'] || {}
          if @opts && !@opts.kind_of?(Hash)
            raise ArgumentError.new("Request opts must be a Hash")
          end
        elsif spec.kind_of?(Proc)
          @topic = spec.to_s
        else
          raise ArgumentError.new("Request spec must be a spec or proc")
        end
      end

      def execute(data = {}, &blk)
        if @spec.kind_of?(Proc)
          begin
            res = @spec.call(data)
          rescue => e
            code = 500
          end
          code = res ? 200 : 500
          blk.call(res, code)
        else
          message = if @message.kind_of?(Hash) && data.kind_of?(Hash)
                      data.merge(@message)
                    else
                      data
                    end
          @backend.request!(message, @topic, @opts, &blk)
        end
      rescue Exception => e
        GLogger.debug e.message
        GLogger.debug e.backtrace
      end
    end

    class Pipeline #:nodoc:
      attr_reader :pipeline

      def initialize(backend, spec)
        @backend = backend
        unless spec.kind_of? Array
          raise ArgumentError.new("Compose spec must be an array")
        end
        @pipeline = spec.map do |s|
          if s.kind_of?(Pipeline) || s.kind_of?(Request)
            s
          else
            Request.new(backend, s)
          end
        end
      end

      def execute
        raise NotImplementedError.new
      end

      def continuation(queue)
        self.class.new(@backend, queue)
      end
    end

    class Compose < Pipeline
      # Execute a pipeline. The output of the last stage of the
      # pipeline is passed to the block, if all stages are
      # successful. Otherwise, the output of the last successful stage
      # is passed, along with the conitnuation which represents the
      # remaining pipeline
      def execute(msg = {}, &blk)
        blk.call(nil, nil) if pipeline.empty?
        handler = proc do |queue, data, code|
          if queue.empty? || code != 200
            blk.call(data, code, continuation(queue))
          else
            head = queue.first
            tail = queue[1..-1]
            head.execute(data, &handler.curry[tail])
          end
        end
        handler.call(pipeline, msg, 200)
      end
    end

    # Create a Compose pipeline as per the spec. This 
    # is roughly equivalent to the following unix construct
    #   cmd1 | cmd2 | cmd3 ...
    # The spec is an array of hashes, each describing
    # a request topic and message. The message is optional.
    # If present, this message is merged into the output
    # of the previous step, before passing it as the input.
    # Eg, below, if msg2 was a Hash, it would be merged into
    # the output from the subscriber of topic1, and the merged hash
    # would be sent as the request body to topic2
    #    [
    #      {topic: topic1, message: msg1},
    #      {topic: topic2, message: msg2},
    #      ...
    #    ]
    #
    # Instead of a hash, a spec item can also be a callable
    # (proc or lambda). In that case, the output of the previous step
    # is passed through the callable, and the return value of the callable
    # is sent as the input to the next step.
    #
    # In place of a hash or callable, it is also possible to have
    # any kind of composition itself, allowing the creation of
    # nested compositions. See examples in the source code.
    #
    def compose(*spec)
      Compose.new(self, spec.flatten)
    end

    class AndAnd < Pipeline
      # Execute the andand pipeline. The output of the last successful
      # step is passed to block, along with the remaining continuation
      # See the documentation of the #andand method for more details
      def execute(data={}, &blk)
        blk.call(nil, nil) if pipeline.empty?
        handler = proc do |queue, data, code|
          if queue.empty? || code != 200
            blk.call(data, code, continuation(queue))
          else
            head = queue.first
            tail = queue[1..-1]
            head.execute(&handler.curry[tail])
          end
        end
        handler.call(pipeline, nil, 200)
      end
    end

    # Same as compose this composition does not pass data from one step to the
    # next. Execution halts on the first error
    # It is roughly equivalent to the unix construct
    #   cmd1 && cmd2 && cmd3

    def andand(*spec)
      AndAnd.new(self, spec.flatten)
    end

    class Batch < Pipeline
      def initialize(backend, spec) #:nodoc:
        super(backend, spec)
        @record = false
      end

      def with_recorder(record=true)
        @record = record
        self
      end
      # Execute the batch pipeline. This pipeline ignores all errors
      # step is passed to block.
      # See the documentation of the #batch method for more details
      def execute(data={}, &blk)
        results = []
        blk.call(nil, nil) if pipeline.empty?
        handler = proc do |queue, data, code|
          results << {data: data, code: code} if @record
          if queue.empty?
            result = @record ? results[1..-1] : data
            blk.call(result, code)
          else
            head = queue.first
            tail = queue[1..-1]
            head.execute(&handler.curry[tail])
          end
        end
        handler.call(pipeline, nil, 200)
      end
    end

    # Same a compose, except that no errors are checked
    # and the pipeline executes all steps unconditionally
    # and sequentially. It is roughly equivalent to the unix construct
    #   cmd1; cmd2; cmd3
    # OR
    #   (cmd1; cmd2; cmd3)
    # Params:
    # +record+:: If this is false, only the output of the last step
    # is passed to the block passed to execute. If true, all outputs
    # are collected in an array and passed to the block
    def batch(*spec)
      Batch.new(self, spec.flatten)
    end

    class Parallel < Pipeline
      def initialize(backend, spec) #:nodoc:
        super(backend, spec)
      end

      # Execute the batch pipeline. This pipeline ignores all errors
      # step is passed to block.
      # See the documentation of the #batch method for more details
      def execute(data=nil, &blk)
        results = []
        blk.call(nil, nil) if pipeline.empty?
        waiters = []
        pipeline.each do |p|
          waiter = Gilmour::Waiter.new
          waiters << waiter
          p.execute do |d, c|
            results << {data: d, code: c}
            waiter.signal
          end
        end 
        waiters.each(&:wait)
        code = results.map { |r| r[:code] }.max
        blk.call(results, code)
      end 
    end

    def parallel(*spec)
      Parallel.new(self, spec.flatten)
    end

    def sync(*spec, &blk)
      Parallel.new(self, spec.flatten).execute do |_res, code|
        res = _res.size == 1 ? _res.first[:data] : _res
        blk.call(res, code)
      end
    end
  end
end
