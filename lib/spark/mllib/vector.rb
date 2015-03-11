module Spark
  module Mllib
    module Vectors

      def self.dense(*args)
        DenseVector.new(*args)
      end

      def self.sparse(*args)
        SparseVector.new(*args)
      end

      def self.parse(data)
        if data.start_with?('[') && data.end_with?(']')
          DenseVector.parse(data)
        elsif data.start_with?('(') && data.end_with?(')')
          SparseVector.parse(data)
        else
          raise ArgumentError, 'Unknow vector.'
        end
      end

      def self.to_vector(data)
        if data.is_a?(SparseVector) || data.is_a?(DenseVector)
          data
        elsif data.is_a?(Array)
          DenseVector.new(data)
        end
      end

    end
  end
end

module Spark
  module Mllib
    # @abstract Parent for all type of vectors
    class VectorBase < VectorAdapter
    end
  end
end

module Spark
  module Mllib
    ##
    # A dense vector represented by a value array.
    #
    # Dense vector is a vector in which most of the elements are non-zero.
    #
    # == Example:
    #   DenseVector.new([1,2,3,4,5]).values
    #   # => [1, 2, 3, 4, 5]
    #
    #   DenseVector.new(1..5).values
    #   # => [1, 2, 3, 4, 5]
    #
    class DenseVector < VectorBase

      def initialize(values)
        super(:dense, values.to_a)
      end

      # Covert string to vector
      #
      #   DenseVector.parse("[1.0,2.0,3.0,4.0,5.0]")
      #
      def self.parse(data)
        unless data =~ /\[[0-9., ]+\]/
          raise ArgumentError, 'Unknow format for DenseVector.'
        end

        data.sub!('[', '')
        data.sub!(']', '')

        data = data.split(',')
        data.map!(&:to_f)

        DenseVector.new(data)
      end

      # Convert vector to string
      #
      #   DenseVector.new([1,2,3,4,5]).to_s
      #   # => "[1.0,2.0,3.0,4.0,5.0]"
      #
      def to_s
        "[#{values.join(',')}]"
      end

      def to_java
        JDenseVector.new(values)
      end

      def self.from_java(object)
        DenseVector.new(object.values)
      end

      def marshal_dump
        values
      end

      def marshal_load(array)
        initialize(array)
      end

    end
  end
end

module Spark
  module Mllib
    ##
    # A sparse vector represented by an index array and an value array.
    #
    # Sparse vector is a vector in which most of the elements are zero.
    #
    # == Example:
    #   SparseVector.new(4, {1 => 1.0, 3 => 5.5}).values
    #   # => [0, 1.0, 0, 5.5]
    #
    #   SparseVector.new(4, [[1, 3], [1.0, 5.5]]).values
    #   # => [0, 1.0, 0, 5.5]
    #
    #   SparseVector.new(4, [1, 3], [1.0, 5.5]).values
    #   # => [0, 1.0, 0, 5.5]
    #
    class SparseVector < VectorBase

      attr_reader :indices

      def initialize(arg1, arg2=nil, arg3=nil)
          super(:sparse, arg1)

          if arg2.is_a?(Hash)
            @indices = arg2.keys
            @values = arg2.values
          else
            @indices = arg2
            @values = arg3
          end

          @indices.zip(@values).each do |(index, value)|
            self[index] = value
          end
      end

      # Covert string to vector
      #
      #   SparseVector.parse("(5,[1,4],[3.0,5.0])")
      #
      def self.parse(data)
        data = data.match(/\(([0-9]+)[ ]*,[ ]*\[([0-9,. ]*)\][ ]*,[ ]*\[([0-9,. ]*)\]\)/)
        if data
          size = data[1].to_i
          indices = data[2].split(',')
          indices.map!(&:to_i)
          values = data[3].split(',')
          values.map!(&:to_f)

          SparseVector.new(size, indices, values)
        else
          raise ArgumentError, 'Unknow format for SparseVector.'
        end
      end

      # Convert vector to string
      #
      #   SparseVector.new(5, {1 => 3, 4 => 5}).to_s
      #   # => "(5,[1,4],[3.0,5.0])"
      #
      def to_s
        "(#{size},[#{indices.join(',')}],[#{values.join(',')}])"
      end

      def marshal_dump
        [size, indices, values]
      end

      def marshal_load(array)
        initialize(array[0], array[1], array[2])
      end

    end
  end
end
