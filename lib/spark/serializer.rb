module Spark
  ##
  # Serializer
  #
  module Serializer

    DEFAULT_COMPRESS = false
    DEFAULT_BATCH_SIZE = 1024
    DEFAULT_SERIALIZER_NAME = 'marshal'

    @@registered = {}

    # Register class and create method for quick access.
    # Class will be available also as __name__ for using
    # in build method (Proc binding problem).
    #
    # == Examples:
    #   register('test1', 'test2', Class)
    #
    #   Spark::Serializer.test1
    #   Spark::Serializer.test2
    #
    #   # Proc binding problem
    #   build { marshal } # => Spark::Serializer::Marshal
    #
    #   marshal = 1
    #   build { marshal } # => 1
    #
    #   build { __marshal__ } # => Spark::Serializer::Marshal
    #
    def self.register(*args)
      klass = args.pop
      args.each do |arg|
        @@registered[arg] = klass
        define_singleton_method(arg.to_sym){|*args| klass.new(*args) }
        define_singleton_method("__#{arg}__".to_sym){|*args| klass.new(*args) }
      end
    end

    def self.find(name)
      @@registered[name.to_s.downcase]
    end

    def self.find!(name)
      klass = find(name)

      if klass.nil?
        raise Spark::SerializeError, "Unknow serializer #{name}."
      end

      klass
    end

    def self.build(text=nil, &block)
      if block_given?
        class_eval(&block)
      else
        class_eval(text.to_s.downcase)
      end
    end

  end
end

# Parent
require 'spark/serializer/base'

# Basic
require 'spark/serializer/oj'
require 'spark/serializer/marshal'
require 'spark/serializer/message_pack'
require 'spark/serializer/text'

# Others
require 'spark/serializer/batched'
require 'spark/serializer/auto_batched'
require 'spark/serializer/compressed'
require 'spark/serializer/pair'
require 'spark/serializer/cartesian'
