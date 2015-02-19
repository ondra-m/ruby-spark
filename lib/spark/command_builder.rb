require 'spark/command_validator'

module Spark
  class CommandBuilder

    extend Forwardable

    include Spark::Helper::Serialize
    include Spark::Helper::System
    include Spark::CommandValidator

    attr_reader :command

    def_delegators :@command, :serializer, :serializer=, :deserializer, :deserializer=, :libraries,
                   :accumulators, :accumulators=

    def initialize(serializer, deserializer=nil)
      @command = Spark::Command.new
      self.serializer   = serializer
      self.deserializer = deserializer || serializer.dup
    end

    # def self.error(message)
    #   raise Spark::CommandError, message
    # end

    # def error(message)
    #   self.class.error(message)
    # end

    # Deep copy without accumulators
    # => prevent recreating Accumulator class
    def deep_copy
      copy = Marshal.load(Marshal.dump(self))
      copy.accumulators = self.accumulators.dup
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
      @command.add_command(comm)
      self
    end

    def add_library(*libraries)
      @command.libraries += libraries
    end

    def add_accumulator(*accumulators)
      @command.accumulators += accumulators
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
          case func.class.name
          when 'String'
            serialize_function_from_string(func)
          when 'Symbol'
            serialize_function_from_symbol(func)
          when 'Proc'
            serialize_function_from_proc(func)
          when 'Method'
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
