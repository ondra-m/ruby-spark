module Spark
  module Serializer
    class Base

      include Spark::Helper::Serialize

      attr_writer :batch_size

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

      def batch_size(size=-1)
        if size == -1
          return @batch_size
        end

        _new = self.dup
        _new.batch_size = size
        _new
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
      #
      # mri:   respond_to?(:iterator) => false
      # jruby: respond_to?(:iterator) => true
      #
      def load(source)
        if source.is_a?(IO)
          load_from_io(source)
        # elsif source.is_a?(Array)
        #   load_from_array(source)
        elsif try(source, :iterator)
          load_from_iterator(source.iterator)
        end
      end

      # Load data from IO. Data must have a format:
      #  
      # +------------+--------+
      # | signed int |  data  |
      # |     4B     |        |
      # +------------+--------+
      #
      def load_from_io(io)
        result = []
        while true
          begin
            result << load_one_from_io(io)
          rescue
            break
          end
        end

        result.flatten!(1) if batched?
        result
      end

      def load_one_from_io(io)
        deserialize(io.read(unpack_int(io.read(4))))
      end

      # def load_from_array(array)
      #   array.map! do |item|
      #     if item.is_a?(String)
      #       # do nothing
      #     else
      #       item = pack_unsigned_chars(item)
      #     end
      #     deserialize(item)
      #   end
      # end

      # Load from Java iterator by calling hasNext and next
      #
      def load_from_iterator(iterator)
        result = []
        while iterator.hasNext
          item = iterator.next
          if item.is_a?(String)
            # do nothing
          else
            item = pack_unsigned_chars(item.to_a)
          end
          result << deserialize(item)
        end

        result.flatten!(1) if batched?
        result
      end

      # ===========================================================================
      # Dump

      # Serialize and send data into IO. Check 'load_from_io' for data format.
      #
      def dump(data, io)
        data = [data] unless data.is_a?(Array)
        data = data.each_slice(batch_size) if batched?
        data.each do |item|
          serialized = serialize(item)
          io.write(pack_int(serialized.size) + serialized)
        end
      end

      def dump_to_java(data)
        data.map! do |item|
          serialize(item).to_java_bytes
        end
      end

      # Rescue cannot be defined
      #
      # mri   => RuntimeError
      # jruby => NoMethodError
      #
      def try(object, method)
        begin
          object.send(method)
          return true
        rescue
          return false
        end
      end

    end
  end
end
