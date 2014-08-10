require "sourcify"

# A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
# partitioned collection of elements that can be operated on in parallel. This class contains the
# basic operations available on all RDDs, such as `map`, `filter`, and `persist`.
#
module Spark
  class RDD

    attr_reader :jrdd, :context, :command

    # Initializing RDD, this method is root of all Pipelined RDD - its unique
    # If you call some operations on this class it will be computed in Java
    #
    #   jrdd: org.apache.spark.api.java.JavaRDD
    #   context: Spark::Context
    #   serializer: Spark::Serializer
    #
    def initialize(jrdd, context, serializer, deserializer=nil)
      @jrdd = jrdd
      @context = context

      @cached = false
      @checkpointed = false

      @command = Spark::Command::Builder.new(serializer, deserializer)
    end


    # =============================================================================
    # Commad

    # Attach method as Symbol or Proc
    def attach(*args)
      @command.add_before(args)
      self
    end

    # Add library which will be loaded on worker
    def add_library(*args)
      @command.add_library(args)
      self
    end

    # Show attached methods, procs and libraries
    def attached
      @command.attached
    end

    # Make a copy of command for new PipelinedRDD
    # .dup and .clone does not do deep copy of @command.template
    def add_command(main, f=nil, options={})
      command = @command.deep_copy
      command.add(main, f, options)
      command
    end


    # =============================================================================
    # Variables and non-computing functions

    def default_reduce_partitions
      @context.config["spark.default.parallelism"] || partitions_size
    end

    def partitions_size
      jrdd.rdd.partitions.size
    end

    # A unique ID for this RDD (within its SparkContext).
    def id
      @jrdd.id
    end

    def cached?
      @cached
    end

    def checkpointed?
      @checkpointed
    end

    # Return the name of this RDD.
    #
    def name
      _name = jrdd.name
      _name && _name.encode(Encoding::UTF_8)
    end

    # Assign a name to this RDD.
    #
    def set_name(name)
      jrdd.setName(name)
    end


    # =============================================================================
    # Actions which return value

    # Return an array that contains all of the elements in this RDD.
    #
    # toArray: mri => Array
    #          jruby => ArrayList
    #
    def collect
      # @command.serializer.load(jrdd.collect.toArray.to_a)
      @command.serializer.load(jrdd.collect)
    end

    # Convert an Array to Hash
    #
    def collect_as_hash
      Hash[collect]
    end

    # Reduces the elements of this RDD using the specified lambda or method.
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.reduce(lambda{|sum, x| sum+x}).collect
    # => 55
    #
    def reduce(f, skip_phase_one=false, options={})
      main = "Proc.new {|iterator| iterator.reduce(&@__main__) }"

      if skip_phase_one
        # All items are send to one worker
        rdd = self
      else
        comm = add_command(main, f, options)
        rdd = PipelinedRDD.new(self, comm)
      end

      # Send all results to one worker and run reduce again
      rdd = rdd.coalesce(1)

      # Add the same function to new RDD
      comm = rdd.add_command(main, f, options)
      comm.deserializer = @command.serializer

      # Value is returned in array
      PipelinedRDD.new(rdd, comm).collect[0]
    end

    # Return the max of this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.max
    # => 10
    #
    def max(skip_phase_one=false)
      self.reduce("lambda{|memo, item| memo > item ? memo : item }", skip_phase_one)
    end

    # Return the min of this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.min
    # => 0
    #
    def min(skip_phase_one=false)
      self.reduce("lambda{|memo, item| memo < item ? memo : item }", skip_phase_one)
    end

    # Return the sum of this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.sum
    # => 55
    #
    def sum(skip_phase_one=false)
      self.reduce("lambda{|sum, item| sum + item }", skip_phase_one)
    end

    # Return the number of values in this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.count
    # => 11
    #
    def count
      self.map_partitions("lambda{|iterator| iterator.size }").sum(true)
    end


    # =============================================================================
    # Transformations of RDD

    # Return a new RDD by applying a function to all elements of this RDD.
    #
    # rdd = $sc.parallelize(0..5)
    # rdd.map(lambda {|x| x*2}).collect
    # => [0, 2, 4, 6, 8, 10]
    #
    def map(f, options={})
      main = "Proc.new {|iterator| iterator.map!{|i| @__main__.call(i)} }"
      comm = add_command(main, f, options)

      PipelinedRDD.new(self, comm)
    end

    # Return a new RDD by first applying a function to all elements of this
    # RDD, and then flattening the results.
    #
    # rdd = $sc.parallelize(0..5)
    # rdd.flat_map(lambda {|x| [x, 1]}).collect
    # => [0, 1, 2, 1, 4, 1, 6, 1, 8, 1, 10, 1]
    #
    def flat_map(f, options={})
      main = "Proc.new {|iterator| iterator.map!{|i| @__main__.call(i)}.flatten }"
      comm = add_command(main, f, options)

      PipelinedRDD.new(self, comm)
    end

    # Return a new RDD by applying a function to each partition of this RDD.
    #
    # rdd = $sc.parallelize(0..10, 2)
    # rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect
    # => [15, 40]
    #
    def map_partitions(f, options={})
      main = "Proc.new {|iterator| @__main__.call(iterator) }"
      comm = add_command(main, f, options)

      PipelinedRDD.new(self, comm)
    end

    # Return a new RDD by applying a function to each partition of this RDD, while tracking the index
    # of the original partition.
    #
    # rdd = $sc.parallelize(0...4, 4)
    # rdd.map_partitions_with_index(lambda{|part, index| part[0] * index}).collect
    # => [0, 1, 4, 9]
    #
    def map_partitions_with_index(f, options={})
      main = "Proc.new {|iterator, index| @__main__.call(iterator, index) }"
      comm = add_command(main, f, options)

      PipelinedRDD.new(self, comm)
    end

    # Return a new RDD containing only the elements that satisfy a predicate.
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.filter(lambda{|x| x.even?}).collect
    # => [0, 2, 4, 6, 8, 10]
    #
    def filter(f, options={})
      main = "Proc.new {|iterator| iterator.select{|i| @__main__.call(i)} }"
      comm = add_command(main, f, options)

      PipelinedRDD.new(self, comm)
    end

    # Return an RDD created by coalescing all elements within each partition into an array.
    #
    # rdd = $sc.parallelize(0..10, 3)
    # rdd.glom.collect
    # => [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9, 10]]
    #
    def glom
      main = "Proc.new {|iterator| [iterator] }"
      comm = add_command(main)

      PipelinedRDD.new(self, comm)
    end

    # Return a new RDD that is reduced into num_partitions partitions.
    #
    # rdd = $sc.parallelize(0..10, 3)
    # rdd.coalesce(2).glom.collect
    # => [[0, 1, 2], [3, 4, 5, 6, 7, 8, 9, 10]]
    #
    def coalesce(num_partitions)
      new_jrdd = jrdd.coalesce(num_partitions)
      RDD.new(new_jrdd, context, @command.serializer, @command.deserializer)
    end

    # Return a new RDD containing the distinct elements in this RDD.
    # Ordering is not preserved because of reducing
    #
    # rdd = $sc.parallelize([1,1,1,2,3])
    # rdd.distinct.collect
    # => [1, 2, 3]
    #
    def distinct
      self.map("lambda{|x| [x, nil]}")
          .reduce_by_key("lambda{|x,_| x}")
          .map("lambda{|x| x[0]}")
    end

    # Return the union of this RDD and another one. Any identical elements will appear multiple
    # times (use .distinct to eliminate them).
    #
    # rdd = $sc.parallelize([1, 2, 3])
    # rdd.union(rdd).collect
    # => [1, 2, 3, 1, 2, 3]
    #
    def union(other)
      if command.deserializer == other.command.deserializer
        new_jrdd = jrdd.union(other.jrdd)
        RDD.new(new_jrdd, context, @command.serializer, @command.deserializer)
      else
        # not yet
      end
    end

    # Return a copy of the RDD partitioned using the specified partitioner.
    #
    # rdd = $sc.parallelize(["1","2","3","4","5"]).map(lambda {|x| [x, 1]})
    # rdd.partitionBy(2).glom.collect
    # => [[["3", 1], ["4", 1]], [["1", 1], ["2", 1], ["5", 1]]]
    #
    def partition_by(num_partitions, partition_func=nil)
      num_partitions ||= default_reduce_partitions
      partition_func ||= "lambda{|x| x.hash}"

      _key_function_ = <<-KEY_FUNCTION
        Proc.new{|iterator|
          iterator.map! {|key, value|
            [@__partition_func__.call(key), [key, value]]
          }
        }
      KEY_FUNCTION

      # RDD is transform from [key, value] to [hash, [key, value]]
      keyed = map_partitions(_key_function_).attach(partition_func: partition_func)
      keyed.command.serializer = Spark::Serializer::Pairwise

      # PairwiseRDD and PythonPartitioner are borrowed from Python
      # but works great on ruby too
      pairwise_rdd = PairwiseRDD.new(keyed.jrdd.rdd).asJavaPairRDD
      partitioner = PythonPartitioner.new(num_partitions, partition_func.object_id)
      jrdd = pairwise_rdd.partitionBy(partitioner).values

      # Prev serializer was Pairwise
      rdd = RDD.new(jrdd, context, Spark::Serializer::Simple)
      rdd
    end


    # =============================================================================
    # Pair functions

    # Merge the values for each key using an associative reduce function. This will also perform
    # the merging locally on each mapper before sending results to a reducer, similarly to a
    # "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
    # parallelism level.
    #
    # rdd = $sc.parallelize(["a","b","c","a","b","c","a","c"]).map(lambda{|x| [x, 1]})
    # rdd.reduce_by_key(lambda{|x,y| x+y}).collect_as_hash
    #
    # => {"a"=>3, "b"=>2, "c"=>3}
    #
    def reduce_by_key(f, num_partitions=nil)
      combine_by_key("lambda {|x| x}", f, f, num_partitions)
    end

    # Generic function to combine the elements for each key using a custom set of aggregation
    # functions. Turns a JavaPairRDD[(K, V)] into a result of type JavaPairRDD[(K, C)], for a
    # "combined type" C * Note that V and C can be different -- for example, one might group an
    # RDD of type (Int, Int) into an RDD of type (Int, List[Int]). Users provide three
    # functions:
    #
    #   createCombiner: which turns a V into a C (e.g., creates a one-element list)
    #   mergeValue: to merge a V into a C (e.g., adds it to the end of a list)
    #   mergeCombiners: to combine two C's into a single one.
    #
    # def combiner(x)
    #   x
    # end
    # def merge(x,y)
    #   x+y
    # end
    # rdd = $sc.parallelize(["a","b","c","a","b","c","a","c"]).map(lambda{|x| [x, 1]})
    # rdd.combine_by_key(:combiner, :merge, :merge).collect_as_hash
    #
    # => {"a"=>3, "b"=>2, "c"=>3}
    #
    def combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions=nil)
      num_partitions ||= default_reduce_partitions

      # Not use combiners[key] ||= ..
      # it tests nil and not has_key?
      _combine_ = <<-COMBINE
        Proc.new{|iterator|
          combiners = {}
          iterator.each do |key, value|
            if combiners.has_key?(key)
              combiners[key] = @__merge_value__.call(combiners[key], value)
            else
              combiners[key] = @__create_combiner__.call(value)
            end
          end
          combiners.to_a
        }
      COMBINE

      _merge_ = <<-MERGE
        Proc.new{|iterator|
          combiners = {}
          iterator.each do |key, value|
            if combiners.has_key?(key)
              combiners[key] = @__merge_combiners__.call(combiners[key], value)
            else
              combiners[key] = value
            end
          end
          combiners.to_a
        }
      MERGE

      combined = map_partitions(_combine_).attach(merge_value: merge_value, create_combiner: create_combiner)
      shuffled = combined.partitionBy(num_partitions)
      shuffled.map_partitions(_merge_).attach(merge_combiners: merge_combiners)
    end

    # Return an RDD with the first element of PairRDD
    #
    # rdd = $sc.parallelize([[1,2], [3,4], [5,6]])
    # rdd.keys.collect
    # => [1, 3, 5]
    #
    def keys
      self.map(lambda{|key, value| key})
    end

    # Return an RDD with the second element of PairRDD
    #
    # rdd = $sc.parallelize([[1,2], [3,4], [5,6]])
    # rdd.keys.collect
    # => [2, 4, 6]
    #
    def values
      self.map(lambda{|key, value| value})
    end



    # Aliases
    alias_method :partitionsSize, :partitions_size
    alias_method :defaultReducePartitions, :default_reduce_partitions
    alias_method :setName, :set_name

    alias_method :flatMap, :flat_map
    alias_method :mapPartitions, :map_partitions
    alias_method :mapPartitionsWithIndex, :map_partitions_with_index
    alias_method :reduceByKey, :reduce_by_key
    alias_method :combineByKey, :combine_by_key
    alias_method :partitionBy, :partition_by
    alias_method :defaultReducePartitions, :default_reduce_partitions

  end

  # Pipelined Resilient Distributed Dataset, operations are pipelined and sended to worker
  #
  # RDD
  # `-- map
  #     `-- map
  #         `-- map
  #
  # Code is executed from top to bottom
  #
  class PipelinedRDD < RDD

    attr_reader :prev_jrdd, :serializer, :command

    def initialize(prev, command)

      if prev.is_a?(PipelinedRDD) && prev.pipelinable?
        # Second, ... stages
        @prev_jrdd = prev.prev_jrdd
      else
        # First stage
        @prev_jrdd = prev.jrdd
      end

      @cached = false
      @checkpointed = false

      @context = prev.context
      @command = command
    end

    def pipelinable?
      !(cached? || checkpointed?)
    end

    # Serialization necessary things and sent it to RubyRDD (scala extension)
    def jrdd
      return @jrdd_values if @jrdd_values

      command = @command.build
      class_tag = @prev_jrdd.classTag

      ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, Spark.worker_dir, class_tag)
      @jrdd_values = ruby_rdd.asJavaRDD
      @jrdd_values
    end

  end
end
