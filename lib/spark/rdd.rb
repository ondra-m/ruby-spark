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

    # jrdd.collect() -> ArrayList
    #     .to_a -> Arrays in Array
    def collect
      bytes_array = jrdd.collect().to_a
      Spark::Serializer::UTF8.load(bytes_array)
    end

    def flat_map(f)
      function = [f, Proc.new {|split, iterator| iterator.map{|i| @_f.call(i)}.flatten }]
      mapPartitionsWithIndex(function)
    end
    alias_method :flatMap, :flat_map

    def mapPartitionsWithIndex(f)
      PipelinedRDD.new(self, f)
    end

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

      command = Marshal.dump(["@_f=#{@function[0].to_source}" , @function[1].to_source]).bytes.to_a
      env = @context.environment
      class_tag = @prev_jrdd.classTag

      ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, env, Spark.ruby_worker, class_tag)
      @jrdd_values = ruby_rdd.asJavaRDD()
      @jrdd_values
    end

  end
end
