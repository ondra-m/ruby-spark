module Spark
  ##
  # Serializer
  #
  class Serializer

    DEFAULT_COMPRESS = false
    DEFAULT_BATCH_SIZE = 1024
    DEFAULT_SERIALIZER_NAME = 'marshal'

    @@registered = {}

    def self.register(*args)
      klass = args.pop
      args.each do |arg|
        @@registered[arg] = klass
        define_singleton_method(arg.to_sym){|*args| klass.new(*args) }
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

  end
end

# Abstract parents
require 'spark/serializer/base'
require 'spark/serializer/basic/base'

# Basic
require 'spark/serializer/basic/oj'
require 'spark/serializer/basic/marshal'
require 'spark/serializer/basic/message_pack'

# # Special
require 'spark/serializer/batched'
require 'spark/serializer/auto_batched'
require 'spark/serializer/compressed'
