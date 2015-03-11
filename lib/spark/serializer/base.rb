module Spark
  module Serializer
    # @abstract Parent for all type of serializers
    class Base

      include Spark::Helper::Serialize
      include Spark::Constant

      attr_reader :batch_size

      # Set default values
      def initialize(batch_size=nil)
        self.batch_size = batch_size
      end

      def ==(other)
        self.class == other.class && self.batch_size == other.batch_size
      end

      # Set values given by user
      def set(batch_size)
        self.batch_size = batch_size unless batch_size.nil?
        self
      end

      def batch_size=(size)
        @batch_size = size.to_i
      end

      def unbatch!
        self.batch_size = 1
      end

      # nil, 0, 1 are considered as non-batched
      def batched?
        batch_size > 1
      end

      # ===========================================================================
      # Load

      # Load and deserialize an Array from IO, Array of Java iterator
      #   mri:   respond_to?(:iterator) => false
      #   jruby: respond_to?(:iterator) => true
      #
      def load(source)
        # Tempfile is Delegator for File so it is not IO
        # second wasy is __getobj__.is_a?(IO)
        if source.is_a?(IO) || source.is_a?(Tempfile)
          load_from_io(source)
        # elsif source.is_a?(Array)
        #   load_from_array(source)
        elsif try(source, :iterator)
          load_from_iterator(source.iterator)
        end
      end

      # Load data from IO. Data must have a format:
      #
      #   +------------+--------+
      #   | signed int |  data  |
      #   |     4B     |        |
      #   +------------+--------+
      #
      def load_from_io(io)
        return to_enum(__callee__, io) unless block_given?

        loop do
          lenght = read_int(io)
          break if lenght == DATA_EOF

          result = load_next_from_io(io, lenght)
          if batched? && result.respond_to?(:each)
            result.each {|item| yield item }
          else
            yield result
          end
        end # loop
      end # load_from_io

      def load_next_from_io(io, lenght)
        deserialize(io.read(lenght))
      end

      # Load from Java iterator by calling hasNext and next
      #
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
            # Java object
            if try(item, :getClass)
              case item.getClass.name
              when '[B'
                # Array of bytes
                result << deserialize(pack_unsigned_chars(item.to_a))
              when 'scala.Tuple2'
                # Tuple2
                result << deserialize(item._1, item._2)
              end
            end
          end

        end

        result.flatten!(1) if batched?
        result
      end

      def read_int(io)
        bytes = io.read(4)
        return DATA_EOF if bytes.nil?
        unpack_int(bytes)
      end

      # ===========================================================================
      # Dump

      # Serialize and send data into IO. Check 'load_from_io' for data format.
      def dump(data, io)
        if !data.is_a?(Array) && !data.is_a?(Enumerator)
          data = [data]
        end
        data = data.each_slice(batch_size) if batched?

        data.each do |item|
          serialized = serialize(item)
          io.write(pack_int(serialized.size) + serialized)
        end

        io.flush
      end

      # For direct serialization
      def dump_to_java(data)
        data.map! do |item|
          serialize(item).to_java_bytes
        end
      end

      # Rescue cannot be defined
      #
      #   mri   => RuntimeError
      #   jruby => NoMethodError
      #
      def try(object, method)
        begin
          object.__send__(method)
          return true
        rescue
          return false
        end
      end

    end
  end
end
