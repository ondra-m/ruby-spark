require "sourcify"

# Resilient Distributed Dataset

module Spark
  class RDD

    attr_reader :jrdd, :context, :serializer

    def initialize(jrdd, context, serializer)
      @jrdd = jrdd
      @context = context
      @serializer = serializer

      @cached = false
      @checkpointed = false
    end



    # =======================================================================    
    # Variables 
    # =======================================================================   

    def default_reduce_partitions
      if @context.conf.contains("spark.default.parallelism")
        @context.default_parallelism
      else
        @jrdd.partitions.size
      end
    end

    def id
      @jrdd.id
    end

    def cached?
      @cached
    end

    def checkpointed?
      @checkpointed
    end



    # =======================================================================
    # Computing functions
    # =======================================================================     

    #
    # Return an array that contains all of the elements in this RDD.
    #
    def collect
      # @serializer.load(jrdd.collect.iterator)
      @serializer.load(jrdd.collect.to_a)
    end


    #
    # Return a new RDD by applying a function to all elements of this RDD.
    #
    # rdd = $sc.parallelize(0..5)
    # rdd.map(lambda {|x| x*2}).collect
    # => [0, 2, 4, 6, 8, 10]
    #
    def map(f)
      function = [to_source(f), "Proc.new {|iterator| iterator.map{|i| @__function__.call(i)} }"]
      PipelinedRDD.new(self, function)
    end

    #
    # Return a new RDD by first applying a function to all elements of this
    # RDD, and then flattening the results.
    #
    # rdd = $sc.parallelize(0..5)
    # rdd.flat_map(lambda {|x| [x, 1]}).collect
    # => [0, 1, 2, 1, 4, 1, 6, 1, 8, 1, 10, 1]
    #
    def flat_map(f)
      function = [to_source(f), "Proc.new {|iterator| iterator.flat_map{|i| @__function__.call(i)} }"]
      PipelinedRDD.new(self, function)
    end

    #
    # Return a new RDD by applying a function to each partition of this RDD.
    #
    # rdd = $sc.parallelize(0..10, 2)
    # rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect
    # => [15, 40]
    #
    def map_partitions(f)
      function = [to_source(f), "Proc.new {|iterator| @__function__.call(iterator) }"]
      PipelinedRDD.new(self, function)
    end

    #
    # Return a new RDD by applying a function to each partition of this RDD, while tracking the index
    # of the original partition.
    #
    # rdd = $sc.parallelize(0...4, 4)
    # rdd.map_partitions_with_index(lambda{|part, index| part[0] * index}).collect
    # => [0, 1, 4, 9]
    #
    def map_partitions_with_index(f)
      function = [to_source(f), "Proc.new {|iterator, index| @__function__.call(iterator, index) }"]
      PipelinedRDD.new(self, function)
    end

    #
    # Return a new RDD containing only the elements that satisfy a predicate.
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.filter(lambda{|x| x.even?}).collect
    # => [0, 2, 4, 6, 8, 10]
    #
    def filter(f)
      function = [to_source(f), "Proc.new {|iterator| iterator.select{|i| @__function__.call(i)} }"]
      PipelinedRDD.new(self, function)
    end


    # def reduce_by_key(f, num_partitions=nil)
    #   combine_by_key(lambda {|x| x}, f, f, num_partitions)
    # end

    # def combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions=nil)
    #   num_partitions ||= default_reduce_partitions
    # end



    # Aliases
    alias_method :flatMap, :flat_map
    alias_method :mapPartitions, :map_partitions
    alias_method :mapPartitionsWithIndex, :map_partitions_with_index
    # alias_method :reduceByKey, :reduce_by_key
    # alias_method :combineByKey, :combine_by_key

    private

      def to_source(f)
        return f if f.is_a?(String)

        begin
          f.to_source
        rescue
          raise Spark::SerializeError, "Function can not be serialized. Instead, use the String."
        end
      end

  end


  class PipelinedRDD < RDD

    attr_reader :prev_jrdd, :serializer, :function

    def initialize(prev, function)

      # if !prev.is_a?(PipelinedRDD) || !prev.pipelinable?
      if prev.is_a?(PipelinedRDD) && prev.pipelinable?
        # Second, ... stages
        @function = prev.function
        @function << function
        @prev_jrdd = prev.prev_jrdd
      else
        # First stage
        @function = [function]
        @prev_jrdd = prev.jrdd
      end

      @cached = false
      @checkpointed = false

      @context = prev.context
      @serializer = prev.serializer
    end

    def pipelinable?
      !(cached? || checkpointed?)
    end

    def jrdd
      return @jrdd_values if @jrdd_values

      command = Marshal.dump([@function, @serializer.to_s]).bytes.to_a
      env = @context.environment
      class_tag = @prev_jrdd.classTag

      ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, env, Spark.worker_dir, class_tag)
      @jrdd_values = ruby_rdd.asJavaRDD
      @jrdd_values
    end

  end
end
