require "method_source"

module Spark
  module Command
    class Builder

      include Spark::Serializer::Helper

      attr_reader :template, :attached

      def initialize(serializer, deserializer=nil)
        deserializer ||= serializer

        @template = Spark::Command::Template.new
        @template.serializer   = serializer
        @template.deserializer = deserializer

        @attached = []
      end

      def serializer;       @template.serializer;       end
      def serializer=(x);   @template.serializer = x;   end
      def deserializer;     @template.deserializer;     end
      def deserializer=(x); @template.deserializer = x; end

      def marshal
        # Java use signed number
        unpack_chars(Marshal.dump(@template))
      end

      def add(main, f=nil, options={})
        stage = Spark::Command::Stage.new
        stage.main = main
        stage.pre = [serialize(:main, f)].flatten.join(";")

        @template.stages << stage
      end

      def add_pre(*args)
        @template.pre << parse_attach(args).flatten.join(";")
      end

      def add_library(*args)
        args.map!(&:to_s)

        @attached += args
        @template.library += args
      end

      private

        # Serialized can be Proc and Method
        #   string: already serialized proc
        #   proc: next method
        #   method: next method
        #
        def serialize(name, f)
          case f.class.name.to_sym
          when :String
            return to_variable(name, f)
          when :Proc
            return to_variable(name, serialize_proc(f))
          when :Symbol
            return serialize_method(name, f)
          end
        end

        # Pass an Array or Hash and serialize item
        #   Hash: {:a => :b}
        #         method :b will be accessible as @__a__
        #
        # def test(x)
        #   x*x
        # end
        # attach = [:test, {:new_test => :test}]
        # parse_attach(attach)
        #
        # => [
        #      ["def test(x)\n  x*x\nend\n", "@__test__=lambda(&method(:test))"], 
        #      ["def test(x)\n  x*x\nend\n", "@__new_test__=lambda(&method(:test))"]
        #    ]
        #
        def parse_attach(attach)
          return if attach.nil?

          attach = [attach] unless attach.is_a?(Array)

          result = []
          attach.flatten.each do |item|
            case item.class.name.to_sym
            when :Symbol
              @attached << item
              result << serialize_method(item, item)
            when :Hash
              item.each do |named, f|
                @attached << named.to_sym
                result << serialize(named, f)
              end
            end
          end
          result
        end

        # Serialize Proc as String
        #
        # lambda{|x| x*x}.to_source
        # => "proc { |x| (x * x) }"
        #
        def serialize_proc(proc)
          begin
            proc.to_source
          rescue
            raise Spark::SerializeError, "Proc can not be serialized. Use String instead."
          end
        end

        # Serialize method as string
        # Method will be accessible as method and proc
        #
        # def test(x)
        #   x*x
        # end
        # serialize_method(:test, :test)
        # 
        # => ["def test(x)\n  x*x\nend\n", "@__test__=lambda(&method(:test))"]
        #
        def serialize_method(name, f)
          begin
            f = f.to_s

            # A::B.c
            # => ['A::B', '.c']
            splitted_f = f.split(/\.(\w+)$/)
            method_name = splitted_f.last

            result = []

            if Thread.current[:__pry__]
              result << Pry::Method.from_str(f).source
            else
              # A::B.c must be called as A::B.method(:c)
              result << (splitted_f.size == 1 ? method(method_name).source : eval(splitted_f[0]).method(method_name).source)
            end
            result << to_variable(name, "lambda(&method(:#{method_name}))")
            result
          rescue
            raise Spark::SerializeError, "Method can not be serialized. Use full path or Proc."
          end
        end

        def to_variable(name, source)
          %{@__#{name}__=#{source}}
        end

    end
  end
end
