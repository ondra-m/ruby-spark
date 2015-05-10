require 'spark/command_validator'

module Spark
  ##
  # Builder for building correct {Spark::Command}
  #
  class CommandBuilder

    extend Forwardable

    include Spark::Helper::Serialize
    include Spark::Helper::System
    include Spark::CommandValidator

    attr_reader :command

    def_delegators :@command, :serializer, :serializer=, :deserializer, :deserializer=, :commands,
                              :commands=, :libraries, :libraries=, :bound_objects, :bound_objects=

    def initialize(serializer, deserializer=nil)
      create_command
      self.serializer   = serializer
      self.deserializer = deserializer || serializer.dup
    end

    def create_command
      @command = Spark::Command.new
    end

    # Do not user Marshal.dump(Marshal.load(self)) because some variables
    # have marshal_dump prepared for worker.
    def deep_copy
      copy = self.dup
      copy.create_command
      copy.serializer    = self.serializer.deep_copy
      copy.deserializer  = self.deserializer.deep_copy
      copy.commands      = self.commands.dup
      copy.libraries     = self.libraries.dup
      copy.bound_objects = self.bound_objects.dup
      copy
    end

    # Serialize Command class for worker
    # Java use signed number
    def build
      unpack_chars(Marshal.dump(@command))
    end

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
      @command.commands << comm
      self
    end

    def add_library(*libraries)
      @command.libraries += libraries
    end

    def bind(objects)
      objects.symbolize_keys!
      @command.bound_objects.merge!(objects)
    end

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
          case func
          when String
            serialize_function_from_string(func)
          when Symbol
            serialize_function_from_symbol(func)
          when Proc
            serialize_function_from_proc(func)
          when Method
            serialize_function_from_method(func)
          else
            raise Spark::CommandError, 'You must enter String, Symbol, Proc or Method.'
          end
        end

        def serialize_function_from_string(string)
          {type: 'proc', content: string}
        end

        def serialize_function_from_symbol(symbol)
          {type: 'symbol', content: symbol}
        end

        # Serialize Proc as String
        #
        #   lambda{|x| x*x}.to_source
        #   # => "proc { |x| (x * x) }"
        #
        def serialize_function_from_proc(proc)
          serialize_function_from_string(proc.to_source)
        rescue
          raise Spark::SerializeError, 'Proc can not be serialized. Use String instead.'
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
        def serialize_function_from_method(meth)
          if pry?
            meth = Pry::Method.new(meth)
          end

          {type: 'method', name: meth.name, content: meth.source}
        rescue
          raise Spark::SerializeError, 'Method can not be serialized. Use full path or Proc.'
        end

  end
end
