require "method_source"
require "sourcify"

module Spark
  class CommandBuilder
    
    include Spark::Serializer::Helper
    include Spark::Helper::Platform

    BANNED_NAMES = ['main', 'before', 'libraries', 'exec_function', 'initialize', 'execute']

    def initialize(serializer, deserializer=nil)
      @command = Spark::Command.new
      self.serializer   = serializer
      self.deserializer = deserializer || serializer.dup
    end

    def serializer;       @command.serializer;       end
    def serializer=(x);   @command.serializer = x;   end
    def deserializer;     @command.deserializer;     end
    def deserializer=(x); @command.deserializer = x; end

    # Serialize Command class for worker
    # Java use signed number
    def build
      unpack_chars(Marshal.dump(@command))
    end

    def deep_copy
      Marshal.load(Marshal.dump(self))
    end

    def add_command(main)
      new_command = Spark::Command::Task.new
      new_command.exec_function = main
      @command.add_command(new_command)
      self
    end

    def attach_main_function(func)
      @command.last << serialize(:main, func) if !func.nil?
      self
    end

    def attach_function(*args)
      @command.last << parse(*args)
      self
    end

    def attach_library(*args)
      @command.libraries << args.flatten
      self
    end

    private

        def validate_name!(name)
          if BANNED_NAMES.include?(name)
            raise Spark::SerializeError, "Name #{name} is reserved."
          end
        end

        # Return serialized methods or lambda. Use Hash for named serialization
        # or Array.
        #
        #   def test(x)
        #     x*x
        #   end
        #   func = lambda{|x| x*x}
        #   parse(:test, test2: func)
        #
        #   # => "def test(x)\n  x*x\nend\n@__test__=lambda(&method(:test))\r\n
        #   #    @__test2__=proc { |x| (x * x) }\r\n"
        #
        def parse(*args)
          result = ""
          args.each do |arg|
            case arg.class.name.to_sym
            when :Symbol
              result << serialize(arg, arg)
            when :Hash
              arg.each do |name, func|
                validate_name!(name)
                result << serialize(name, func)
              end
            end
          end
          result
        end

        # Serialized can be Proc and Method
        #
        # === Func
        # * *string:* already serialized proc
        # * *proc:* proc
        # * *symbol:* name of method
        # * *method:* Method class
        #
        def serialize(name, func)
          case func.class.name.to_sym
          when :String
            to_variable(name, func)
          when :Proc
            to_variable(name, serialize_proc(func))
          when :Symbol, :Method
            serialize_method(name, func)
          end + "\r\n"
        end


        # Serialize Proc as String
        #
        #   lambda{|x| x*x}.to_source
        #   # => "proc { |x| (x * x) }"
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
        #   def test(x)
        #     x*x
        #   end
        #   serialize_method(:test, :test)
        # 
        #   # => ["def test(x)\n  x*x\nend\n", "@__test__=lambda(&method(:test))"]
        #
        def serialize_method(name, func)
          begin
            if !func.is_a?(Method)
              func = get_method(func)
            end

            result = ""
            result << func.source
            result << to_variable(name, "lambda(&method(:#{func.name}))")
            result
          rescue
            raise Spark::SerializeError, "Method can not be serialized. Use full path or Proc."
          end
        end

        # Return Method based on *func*
        def get_method(func)
          func = func.to_s

          # A::B.c
          # => ['A::B', '.c']
          splitted_func = func.split(/\.(\w+)$/)
          method_name = splitted_func.last

          if pry?
            Pry::Method.from_str(func).source
          else
            # A::B.c must be called as A::B.method(:c)
            if splitted_func.size == 1
              method(method_name)
            else
              eval(splitted_func[0]).method(method_name)
            end
          end
        end

        def to_variable(name, source)
          %{@__#{name}__=#{source}}
        end

  end
end
