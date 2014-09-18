require "method_source"
require "sourcify"

require "spark/command_validator"

module Spark
  class CommandBuilder

    include Spark::Serializer::Helper
    include Spark::Helper::Platform
    include Spark::CommandValidator

    def initialize(serializer, deserializer=nil)
      @command = Spark::Command.new
      self.serializer   = serializer
      self.deserializer = deserializer || serializer.dup
    end

    def serializer;       @command.serializer;       end
    def serializer=(x);   @command.serializer = x;   end
    def deserializer;     @command.deserializer;     end
    def deserializer=(x); @command.deserializer = x; end

    def self.error(message)
      raise Spark::CommandError, message
    end

    def error(message)
      self.class.error(message)
    end

    # Serialize Command class for worker
    # Java use signed number
    def build
      unpack_chars(Marshal.dump(@command))
    end

    # def deep_copy
    #   Marshal.load(Marshal.dump(self))
    # end

    # def add_task(type, args)
    #   task = Spark::Command.const_get("#{type.to_s.camelize}Task").new(args)
    #   @command.add_task(task)
    #   self
    # end

    def add_command(klass, *args)
      variables = klass.settings.variables
      validate_size(variables, args)

      built_args = []
      variables.values.zip(args) do |var, arg|
        if var[:function]
          arg = serialize_function(arg)
        end

        validate(arg, var)
        built_args << arg
      end

      comm = klass.new(*built_args)
      @command.add_command(comm)
      self
    end

    # def attach_function(*args)
    #   @command.last << parse(true, *args)
    #   self
    # end

    # # The same as `attach_function` but without validation. 
    # # This should be used only from RDD.
    # def attach_function!(*args)
    #   @command.last << parse(false, *args)
    #   self
    # end

    # def attach_library(*args)
    #   @command.libraries << args.flatten
    #   self
    # end

    private

        # Serialized can be Proc and Method
        #
        # === Func
        # * *string:* already serialized proc
        # * *proc:* proc
        # * *symbol:* name of method
        # * *method:* Method class
        #
        def serialize_function(func)
          case func.class.name.to_sym
          when :String
            serialize_function_from_string(fun)
          when :Proc
            serialize_function_from_proc(func)
          when :Symbol
            serialize_function_from_symbol(func)
          when :Method
            serialize_function_from_method(func)
          else
            nil
          end
        end

        def serialize_function_from_string(func)
          {type: "proc", content: func}
        end

        # Serialize Proc as String
        #
        #   lambda{|x| x*x}.to_source
        #   # => "proc { |x| (x * x) }"
        #
        def serialize_function_from_proc(proc)
          serialize_function_from_string(proc.to_source)
        rescue
          raise Spark::SerializeError, "Proc can not be serialized. Use String instead."
        end

        # Symbol represent name of the method
        #
        #   def test(x)
        #     x*x
        #   end
        #   serialize_function_from_symbol(:test)
        #
        #   # => "def test(x)\n  x*x\nend\n"
        #
        def serialize_function_from_symbol(symbol)
          symbol = symbol.to_s

          # A::B.c
          # => ['A::B', '.c']
          splitted_func = symbol.split(/\.(\w+)$/)
          method_name = splitted_func.last

          if pry?
            func = Pry::Method.from_str(symbol)
          else
            # A::B.c must be called as A::B.method(:c)
            if splitted_func.size == 1
              func = method(method_name)
            else
              func = eval(splitted_func[0]).method(method_name)
            end
          end
          
          serialize_function_from_method(func)
        end

        # Serialize method as string
        #
        #   def test(x)
        #     x*x
        #   end
        #   serialize_function_from_method(method(:test))
        # 
        #   # => "def test(x)\n  x*x\nend\n"
        #
        def serialize_function_from_method(func)
          {type: "method", name: func.name, content: func.source}
        # rescue
          # raise Spark::SerializeError, "Method can not be serialized. Use full path or Proc."
        end

  end
end
