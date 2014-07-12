require "sourcify"

# Resilient Distributed Dataset

module Spark
  class RDD

    attr_reader :jrdd, :context

    def initialize(jrdd, context)
      @jrdd = jrdd
      @context = context

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



    # =======================================================================    
    # Compute functions    
    # =======================================================================        


    # jrdd.collect() -> ArrayList
    #     .to_a -> Arrays in Array
    def collect
      bytes_array = jrdd.collect().to_a
      Spark::Serializer::UTF8.load(bytes_array)
    end

    def map(f)
      function = [f, Proc.new {|split, iterator| iterator.map{|i| @_f.call(i)} }]
      PipelinedRDD.new(self, function)
    end

    def flat_map(f)
      function = [f, Proc.new {|split, iterator| iterator.map{|i| @_f.call(i)}.flatten }]
      map_partitions_with_index(function)
    end

    def reduce_by_key(f, num_partitions=nil)
      combine_by_key(lambda {|x| x}, f, f, num_partitions)
    end

    def combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions=nil)
      num_partitions ||= default_reduce_partitions
    end

    def map_partitions_with_index(f)
      PipelinedRDD.new(self, f)
    end



    # Aliases
    alias_method :flatMap, :flat_map
    alias_method :reduceByKey, :reduce_by_key
    alias_method :combineByKey, :combine_by_key
    alias_method :mapPartitionsWithIndex, :map_partitions_with_index

  end


  class PipelinedRDD < RDD

    def initialize(prev, function)
      @function = function
      @prev_jrdd = prev.jrdd

      @cached = false
      @checkpointed = false

      @context = prev.context
    end

    def jrdd
      return @jrdd_values if @jrdd_values

      command = Marshal.dump(["@_f=#{@function[0].to_source}", @function[1].to_source]).bytes.to_a
      env = @context.environment
      class_tag = @prev_jrdd.classTag

      ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, env, Spark.worker_dir, class_tag)
      @jrdd_values = ruby_rdd.asJavaRDD()
      @jrdd_values
    end

  end
end
