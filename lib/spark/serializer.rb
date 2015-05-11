module Spark
  ##
  # Serializer
  #
  class Serializer

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

    def self.build(&block)
      class_eval(&block)
    end

  end
end

# Parents
require 'spark/serializer/simple'
require 'spark/serializer/basic/base'

# Basic
require 'spark/serializer/basic/oj'
require 'spark/serializer/basic/marshal'
require 'spark/serializer/basic/message_pack'
require 'spark/serializer/basic/text'

# Others
require 'spark/serializer/batched'
require 'spark/serializer/auto_batched'
require 'spark/serializer/compressed'
