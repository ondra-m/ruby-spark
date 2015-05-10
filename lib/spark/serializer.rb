module Spark
  ##
  # Serializer
  #
  # Dump and load methods have a different functionality on chain's serializators.
  # Dump:: serialize all data
  # Load:: deserialize only one item
  #
  class Serializer
    # # Abstract
    # autoload :Base,        'spark/serializer/base'

    # # Basic
    # autoload :Oj,          'spark/serializer/basic/oj'
    # autoload :Marshal,     'spark/serializer/basic/marshal'
    # autoload :MessagePack, 'spark/serializer/basic/message_pack'


    # autoload :UTF8,        'spark/serializer/utf8'
    # autoload :Pair,        'spark/serializer/pair'
    # autoload :Cartesian,   'spark/serializer/cartesian'

    # DEFAULT_BATCH_SIZE = 1024
    # DEFAULT_SERIALIZER_NAME = 'marshal'

    # def self.get(suggestion)
    #   const_get(suggestion.to_s.camelize) rescue nil
    # end

    # def self.get!(suggestion)
    #   const_get(suggestion.to_s.camelize)
    # rescue
    #   raise Spark::NotImplemented, "Serializer #{suggestion.to_s.camelize} not exist."
    # end




    DEFAULT_BATCH_SIZE = 1024
    DEFAULT_SERIALIZER_NAME = 'marshal'

    attr_reader :chain

    @@registered = {}

    def self.register(*args)
      klass = args.pop
      args.each do |arg|
        @@registered[arg] = klass
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


    def initialize
      @chain = []
    end

    def add(ser, *args)
      if ser.is_a?(String) || ser.is_a?(Symbol)
        ser = Serializer.find!(ser)
        ser = ser.new(*args)
      end

      @chain << ser
      self
    end

    def inspect
      result  = "#<Spark::Serializer:0x#{object_id}  "
      result << chain.map(&:to_s).join(' -> ')
      result << '>'
      result
    end

    def check_each_respond(data)
      unless data.respond_to?(:each)
        raise Spark::SerializeError, 'Data must respond to each.'
      end
    end


    # === Dump ================================================================

    def dump_to_io(data, io)
      # Must be an Array, Enumerator, Range, ...
      check_each_respond(data)

      # Lazy serialization
      data = data.lazy
      @chain.each do |ser|
        data = ser.dump(data)
      end

      # Write to IO
      data.each do |item|
        # Size and data can have different encoding
        # Marshal: both ASCII
        # Oj: ASCII and UTF-8
        io.write_int(item.bytesize)
        io.write(item)
      end

      io.flush
    end


    # === Load ================================================================

    def load_from_io(io)
      # Lazy deserialization
      return to_enum(__callee__, io) unless block_given?

      loop do
        size = io.read_int_or_eof
        break if size == Spark::Constant::DATA_EOF

        # Load one item
        data = io.read(size)
        @chain.reverse.each do |ser|
          data = ser.load(data, io)
        end

        # All serializator should return an Array
        if data.respond_to?(:each)
          data.each {|item| yield item }
        else
          yield data
        end
      end
    end

    def load_from_iterator(iterator)
      result = []
      while iterator.hasNext
        item = iterator.next

        # mri: data are String
        # jruby: data are bytes Array

        if item.is_a?(String)
          # Serialized data
          result << deserialize(item)
        else
          case item.getClass.getSimpleName
          when 'byte[]'
            result << deserialize(pack_unsigned_chars(item.to_a))
          when 'Tuple2'
            result << deserialize(item._1, item._2)
          else
            raise Spark::SerializeError, "Cannot deserialize #{item.getClass.getSimpleName} class."
          end
        end
      end
    end






      # Load from Java iterator by calling hasNext and next
      #
      def load_from_iterator(iterator)
        unless Spark.jb.java_object?(iterator)
          raise Spark::SerializeError, "Class #{iterator.class} is not a Java object."
        end

        result = []
        while iterator.hasNext
          item = iterator.next

          # mri: data are String
          # jruby: data are bytes Array

          if item.is_a?(String)
            # Serialized data
            result << deserialize(item)
          else
            case item.getClass.getSimpleName
            when 'byte[]'
              result << deserialize(pack_unsigned_chars(item.to_a))
            when 'Tuple2'
              result << deserialize(item._1, item._2)
            else
              raise Spark::SerializeError, "Cannot deserialize #{item.getClass.getSimpleName} class."
            end
          end
        end

        result.flatten!(1) if batched?
        result
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

# Special
require 'spark/serializer/batched'
require 'spark/serializer/auto_batched'
require 'spark/serializer/compressed'
