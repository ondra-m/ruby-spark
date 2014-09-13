##
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

      @command = Spark::CommandBuilder.new(serializer, deserializer)
    end


    # =============================================================================
    # Commad and serializer

    # Attach method as Symbol or Proc
    def attach_function(*args)
      @command.attach_function(*args)
      self
    end

    # Add library which will be loaded on worker
    def attach_library(*args)
      @command.attach_library(*args)
      self
    end

    # Make a copy of command for new PipelinedRDD
    # .dup and .clone does not do deep copy of @command.template
    #
    # Method should be private but _reduce need it public to
    # avoid recursion (and Stack level too deep)
    #
    def add_task(args, main_func=nil, options={})
      add_task_by_type(:simple, args).attach_function!(main: main_func)
    end

    def add_task_by_type(type, args, options={})
      @command.deep_copy
              .add_task(type, args)
    end

    def serializer
      @command.serializer
    end

    def deserializer
      @command.deserializer
    end

    # =============================================================================
    # Variables and non-computing functions

    def default_reduce_partitions
      @context.config["spark.default.parallelism"] || partitions_size
    end

    # Count of ParallelCollectionPartition
    def partitions_size
      jrdd.rdd.partitions.size
    end

    # A unique ID for this RDD (within its SparkContext).
    def id
      jrdd.id
    end

    # Persist this RDD with the default storage level MEMORY_ONLY_SER because of serialization.
    def cache
      persist("memory_only_ser")
    end

    # Set this RDD's storage level to persist its values across operations after the first time
    # it is computed. This can only be used to assign a new storage level if the RDD does not
    # have a storage level set yet.
    #
    # See StorageLevel for type of new_level
    #
    def persist(new_level)
      @cached = true
      jrdd.persist(Spark::StorageLevel.java_get(new_level))
      self
    end

    # Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
    #
    #   blocking: whether to block until all blocks are deleted.
    #
    def unpersist(blocking=true)
      @cached = false
      jrdd.unpersist(blocking)
      self
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
    # RJB raise an error if stage is killed.
    #
    # toArray: mri => Array
    #          jruby => ArrayList
    #
    def collect
      # @command.serializer.load(jrdd.collect.toArray.to_a)
      @command.serializer.load(jrdd.collect)
    rescue
      nil
    end

    # Convert an Array to Hash
    #
    def collect_as_hash
      Hash[collect]
    end

    # Reduces the elements of this RDD using the specified lambda or method.
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.reduce(lambda{|sum, x| sum+x})
    # => 55
    #
    def reduce(f, options={})
      main = "Proc.new {|iterator| [iterator.reduce(&@__main__)] }"
      _reduce(main, f, f, options)
    end

    # Aggregate the elements of each partition, and then the results for all the partitions, using a
    # given associative function and a neutral "zero value".
    #
    # The function f(x, y) is allowed to modify x and return it as its result value to avoid 
    # object allocation; however, it should not modify y.
    #
    # Be careful, zero_values is applied to all stages. See example.
    #
    # rdd = $sc.parallelize(0..10, 2)
    # rdd.fold(1, lambda{|sum, x| sum+x})
    # => 58
    #
    def fold(zero_value, f, options={})
      self.aggregate(zero_value, f, f, options)
    end

    # Aggregate the elements of each partition, and then the results for all the partitions, using
    # given combine functions and a neutral "zero value".
    #
    # This function can return a different result type. We need one operation for merging.
    #
    # Result must be an Array otherwise Serializer Array's zero value will be send
    # as multiple values and not just one.
    #
    # 1 2 3 4 5  => 15 + 1 = 16
    # 6 7 8 9 10 => 40 + 1 = 41
    # 16 * 41 = 656
    #
    # seq = lambda{|x,y| x+y}
    # com = lambda{|x,y| x*y}
    #
    # rdd = $sc.parallelize(1..10, 2)
    # rdd.aggregate(1, seq, com)
    # => 656
    #
    def aggregate(zero_value, seq_op, comb_op, options={})
      main = <<-MAIN
        Proc.new { |iterator|
          [iterator.reduce(Marshal.load("#{Marshal.dump(zero_value)}"), &@__main__)]
        }
      MAIN
      _reduce(main, seq_op, comb_op, options)
    end

    # Return the max of this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.max
    # => 10
    #
    def max
      self.reduce("lambda{|memo, item| memo > item ? memo : item }")
    end

    # Return the min of this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.min
    # => 0
    #
    def min
      self.reduce("lambda{|memo, item| memo < item ? memo : item }")
    end

    # Return the sum of this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.sum
    # => 55
    #
    def sum
      self.reduce("lambda{|sum, item| sum + item}")
    end

    # Return the number of values in this RDD
    #
    # rdd = $sc.parallelize(0..10)
    # rdd.count
    # => 11
    #
    def count
      self.map_partitions("lambda{|iterator| iterator.size }")
          .aggregate(0, nil, "lambda{|sum, item| sum + item }")
    end

    # Applies a function f to all elements of this RDD.
    #
    # rdd = $sc.parallelize(0..5)
    # rdd.foreach(lambda{|x| puts x})
    # => nil
    #
    def foreach(f, options={})
      main = "Proc.new {|iterator| iterator.each {|_item_| @__main__.call(_item_)}; nil}"
      comm = add_task(main, f, options)

      PipelinedRDD.new(self, comm).collect
      nil
    end

    # Applies a function f to each partition of this RDD.
    #
    # rdd = $sc.parallelize(0..5)
    # rdd.foreachPartition(lambda{|x| puts x.to_s})
    # => nil
    #
    def foreach_partition(f, options={})
      main = "Proc.new {|iterator| @__main__.call(iterator); nil }"
      comm = add_task(main, f, options)

      PipelinedRDD.new(self, comm).collect
      nil
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
      comm = add_task(main, f, options)

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
      comm = add_task(main, f, options)

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
      comm = add_task(main, f, options)

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
      comm = add_task(main, f, options)

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
      comm = add_task(main, f, options)

      PipelinedRDD.new(self, comm)
    end

    # Return a new RDD containing non-nil elements.
    #
    # rdd = $sc.parallelize([1, nil, 2, nil, 3])
    # rdd.compact.collect
    # => [1, 2, 3]
    #
    def compact
      main = "Proc.new {|iterator| iterator.compact!; iterator }"
      comm = add_task(main)

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
      comm = add_task(main)

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
      if self.serializer != other.serializer
        other = other.reserialize(serializer.name, serializer.batch_size)
      end

      new_jrdd = jrdd.union(other.jrdd)
      RDD.new(new_jrdd, context, serializer, deserializer)
    end

    # Return a new RDD with different serializer. This method is useful during union
    # and join operations.
    #
    # rdd = $sc.parallelize([1, 2, 3], nil, serializer: "marshal")
    # rdd = rdd.map(lambda{|x| x.to_s})
    # rdd.reserialize("oj").collect
    # => ["1", "2", "3"]
    #
    def reserialize(new_serializer, new_batch_size=nil)
      new_batch_size ||= deserializer.batch_size
      new_serializer = Spark::Serializer.get!(new_serializer).new(new_batch_size)

      if serializer == new_serializer
        return self
      end

      new_command = @command.deep_copy
      new_command.serializer = new_serializer

      PipelinedRDD.new(self, new_command)
    end

    # Return the intersection of this RDD and another one. The output will not contain
    # any duplicate elements, even if the input RDDs did.
    #
    # rdd1 = $sc.parallelize([1,2,3,4,5])
    # rdd2 = $sc.parallelize([1,4,5,6,7])
    # rdd1.intersection(rdd2).collect
    # => [1, 4, 5]
    #
    def intersection(other)
      mapping_function = "Proc.new {|item| [item, nil]}"
      filter_function  = "Proc.new {|key, values| values.size > 1}"

      self.map(mapping_function)
          .cogroup(other.map(mapping_function))
          .filter(filter_function)
          .keys
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

      key_function = <<-KEY_FUNCTION
        Proc.new{|iterator|
          iterator.map! {|key, value|
            [pack_long(@__partition_func__.call(key)), [key, value]]
          }.flatten!(1);
          iterator
        }
      KEY_FUNCTION

      # RDD is transform from [key, value] to [hash, [key, value]]
      keyed = map_partitions(key_function).attach_function(partition_func: partition_func)
      keyed.serializer.unbatch!

      # PairwiseRDD and PythonPartitioner are borrowed from Python
      # but works great on ruby too
      pairwise_rdd = PairwiseRDD.new(keyed.jrdd.rdd).asJavaPairRDD
      partitioner = PythonPartitioner.new(num_partitions, partition_func.object_id)
      new_jrdd = pairwise_rdd.partitionBy(partitioner).values

      # Reset deserializer
      RDD.new(new_jrdd, context, @command.serializer, keyed.serializer)
    end

    def sample(with_replacement, fraction, seed=nil)
      if with_replacement
        sampler = Spark::Sampler::Poisson
      else
        sampler = Spark::Sampler::Uniform
      end
      comm = add_task_by_type(:sample, sampler.new(fraction, seed))

      PipelinedRDD.new(self, comm)
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
      combine = <<-COMBINE
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

      merge = <<-MERGE
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

      combined = map_partitions(combine).attach_function(merge_value: merge_value, create_combiner: create_combiner)
      shuffled = combined.partition_by(num_partitions)
      shuffled.map_partitions(merge).attach_function(merge_combiners: merge_combiners)
    end

    # Group the values for each key in the RDD into a single sequence. Allows controlling the
    # partitioning of the resulting key-value pair RDD by passing a Partitioner.
    #
    # Note: If you are grouping in order to perform an aggregation (such as a sum or average) 
    # over each key, using reduce_by_key or combine_by_key will provide much better performance.
    #
    # rdd = $sc.parallelize([["a", 1], ["a", 2], ["b", 3]])
    # rdd.group_by_key.collect
    # => [["a", [1, 2]], ["b", [3]]]
    #
    def group_by_key(num_partitions=nil)
      create_combiner = "Proc.new {|item| [item]}"
      merge_value     = "Proc.new {|combiner, item| combiner << item; combiner}"
      merge_combiners = "Proc.new {|combiner_1, combiner_2| combiner_1 += combiner_2; combiner_1}"

      combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions)
    end

    # The same functionality as cogroup but this can grouped only 2 rdd's and you
    # can change num_partitions.
    #
    # rdd1 = $sc.parallelize([["a", 1], ["a", 2], ["b", 3]])
    # rdd2 = $sc.parallelize([["a", 4], ["a", 5], ["b", 6]])
    # rdd1.group_with(rdd2).collect
    # => [["a", [1, 2, 4, 5]], ["b", [3, 6]]]
    #
    def group_with(other, num_partitions=nil)
      self.union(other).group_by_key(num_partitions)
    end

    # For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
    # list of values for that key in `this` as well as `other`.
    #
    # rdd1 = $sc.parallelize([["a", 1], ["a", 2], ["b", 3]])
    # rdd2 = $sc.parallelize([["a", 4], ["a", 5], ["b", 6]])
    # rdd3 = $sc.parallelize([["a", 7], ["a", 8], ["b", 9]])
    # rdd1.cogroup(rdd2, rdd3).collect
    # => [["a", [1, 2, 4, 5, 7, 8]], ["b", [3, 6, 9]]]
    #
    def cogroup(*others)
      unioned = self
      others.each do |other|
        unioned = unioned.union(other)
      end

      unioned.group_by_key
    end

    # Pass each value in the key-value pair RDD through a map function without changing
    # the keys. This also retains the original RDD's partitioning.
    #
    # rdd = $sc.parallelize(["ruby", "scala", "java"])
    # rdd = rdd.map(lambda{|x| [x, x]})
    # rdd = rdd.map_values(lambda{|x| x.upcase})
    # rdd.collect
    # => [["ruby", "RUBY"], ["scala", "SCALA"], ["java", "JAVA"]]
    #
    def map_values(f)
      func = "Proc.new {|key, value| [key, @__mapping__.call(value)]}"
      self.map(func).attach_function(mapping: f)
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
    alias_method :groupByKey, :group_by_key
    alias_method :groupWith, :group_with
    alias_method :partitionBy, :partition_by
    alias_method :defaultReducePartitions, :default_reduce_partitions
    alias_method :foreachPartition, :foreach_partition
    alias_method :mapValues, :map_values

    private

      # This is base method for reduce operation. Is used by reduce, fold and aggregation.
      # Only difference is that fold has zero value.
      #
      def _reduce(main, seq_op, comb_op, options={})
        if seq_op.nil?
          # Partitions are already reduced
          rdd = self
        else
          comm = add_task(main, seq_op, options)
          rdd = PipelinedRDD.new(self, comm)
        end

        # Send all results to one worker and combine results
        rdd = rdd.coalesce(1).compact

        # Add the same function to new RDD
        comm = rdd.add_task(main, comb_op, options)
        comm.deserializer = @command.serializer

        # Value is returned in array
        PipelinedRDD.new(rdd, comm).collect[0]
      end

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

    attr_reader :prev_jrdd, :command

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
      @jrdd ||= _jrdd
    end

    private

      def _jrdd
        command = @command.build
        class_tag = @prev_jrdd.classTag

        ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, Spark.worker_dir, class_tag)
        ruby_rdd.asJavaRDD
      end

  end
end
