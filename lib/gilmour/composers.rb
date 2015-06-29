
module Gilmour
  module Composers
    class Request
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
          res = @spec.call(data)
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

    class Pipeline
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

    def compose(spec)
      Compose.new(self, spec)
    end

    class AndAnd < Pipeline
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

    def andand(spec)
      AndAnd.new(self, spec)
    end

    class Batch < Pipeline
      def initialize(backend, spec, record=false)
        super(backend, spec)
        @record = record
      end

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

    def batch(spec, record=false)
      Batch.new(self, spec, record)
    end
  end
end
