module Spark
  ##
  # A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
  # partitioned collection of elements that can be operated on in parallel. This class contains the
  # basic operations available on all RDDs, such as `map`, `filter`, and `persist`.
  #
  class RDD

    extend Forwardable

    attr_reader :jrdd, :context, :command

    include Spark::Helper::Logger
    include Spark::Helper::Parser
    include Spark::Helper::Statistic

    def_delegators :@command, :serializer, :deserializer, :libraries, :files

    # Initializing RDD, this method is root of all Pipelined RDD - its unique
    # If you call some operations on this class it will be computed in Java
    #
    # == Parameters:
    # jrdd:: org.apache.spark.api.java.JavaRDD
    # context:: {Spark::Context}
    # serializer:: {Spark::Serializer}
    #
    def initialize(jrdd, context, serializer, deserializer=nil)
      @jrdd = jrdd
      @context = context

      @cached = false
      @checkpointed = false

      @command = Spark::CommandBuilder.new(serializer, deserializer)
    end

    def inspect
      comms = @command.commands.join(' -> ')

      result  = %{#<#{self.class.name}:0x#{object_id}}
      result << %{ (#{comms})} unless comms.empty?
      result << %{ (cached)} if cached?
      result << %{\n}
      result << %{  Serializer: "#{serializer}"\n}
      result << %{Deserializer: "#{deserializer}"}
      result << %{>}
      result
    end


    # =============================================================================
    # Operators

    def +(other)
      self.union(other)
    end


    # =============================================================================
    # Commad and serializer

    def add_command(klass, *args)
      @command.deep_copy.add_command(klass, *args)
    end

    # Add ruby library
    # Libraries will be included before computing
    #
    # == Example:
    #   rdd.add_library('pry').add_library('nio4r', 'distribution')
    #
    def add_library(*libraries)
      @command.add_library(*libraries)
      self
    end

    # Bind object to RDD
    #
    # == Example:
    #   text = "test"
    #
    #   rdd = $sc.parallelize(0..5)
    #   rdd = rdd.map(lambda{|x| x.to_s + " " + text})
    #   rdd = rdd.bind(text: text)
    #
    #   rdd.collect
    #   # => ["0 test", "1 test", "2 test", "3 test", "4 test", "5 test"]
    #
    def bind(objects)
      unless objects.is_a?(Hash)
        raise ArgumentError, 'Argument must be a Hash.'
      end

      @command.bind(objects)
      self
    end

    def new_rdd_from_command(klass, *args)
      comm = add_command(klass, *args)
      PipelinedRDD.new(self, comm)
    end


    # =============================================================================
    # Variables and non-computing functions

    def config
      @context.config
    end

    def default_reduce_partitions
      config['spark.default.parallelism'] || partitions_size
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
      persist('memory_only_ser')
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
    # == Parameters:
    # blocking:: whether to block until all blocks are deleted.
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
    def set_name(value)
      jrdd.setName(value)
      value
    end

    def name=(value)
      set_name(value)
    end

    def to_java
      marshal = Spark::Serializer.marshal

      if deserializer.batched?
        ser = deserializer.deep_copy
        ser.serializer = marshal
      else
        ser = Spark::Serializer.batched(marshal)
      end

      rdd = self.reserialize(ser)
      RubyRDD.toJava(rdd.jrdd, rdd.serializer.batched?)
    end


    # =============================================================================
    # Actions which return value

    # Return an array that contains all of the elements in this RDD.
    # RJB raise an error if stage is killed.
    def collect(as_enum=false)
      file = Tempfile.new('collect', context.temp_dir)

      context.set_call_site(caller.first)
      RubyRDD.writeRDDToFile(jrdd.rdd, file.path)

      collect_from_file(file, as_enum)
    rescue => e
      raise Spark::RDDError, e.message
    ensure
      context.clear_call_site
    end

    def collect_from_file(file, as_enum=false)
      if self.is_a?(PipelinedRDD)
        klass = @command.serializer
      else
        klass = @command.deserializer
      end

      if as_enum
        result = klass.load_from_file(file)
      else
        result = klass.load_from_io(file).to_a
        file.close
        file.unlink
      end

      result
    end

    # Convert an Array to Hash
    #
    def collect_as_hash
      Hash[collect]
    end

    # Take the first num elements of the RDD.
    #
    # It works by first scanning one partition, and use the results from
    # that partition to estimate the number of additional partitions needed
    # to satisfy the limit.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..100, 20)
    #   rdd.take(5)
    #   # => [0, 1, 2, 3, 4]
    #
    def take(count)
      buffer = []

      parts_count = self.partitions_size
      # No parts was scanned, yet
      last_scanned = -1

      while buffer.empty?
        last_scanned += 1
        buffer += context.run_job_with_command(self, [last_scanned], true, Spark::Command::Take, 0, -1)
      end

      # Assumption. Depend on batch_size and how Spark divided data.
      items_per_part = buffer.size
      left = count - buffer.size

      while left > 0 && last_scanned < parts_count
        parts_to_take = (left.to_f/items_per_part).ceil
        parts_for_scanned = Array.new(parts_to_take) do
          last_scanned += 1
        end

        # We cannot take exact number of items because workers are isolated from each other.
        # => once you take e.g. 50% from last part and left is still > 0 then its very
        # difficult merge new items
        items = context.run_job_with_command(self, parts_for_scanned, true, Spark::Command::Take, left, last_scanned)
        buffer += items

        left = count - buffer.size
        # Average size of all parts
        items_per_part = [items_per_part, items.size].reduce(0){|sum, x| sum + x.to_f/2}
      end

      buffer.slice!(0, count)
    end

    # Return the first element in this RDD.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..100)
    #   rdd.first
    #   # => 0
    #
    def first
      self.take(1)[0]
    end

    # Reduces the elements of this RDD using the specified lambda or method.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.reduce(lambda{|sum, x| sum+x})
    #   # => 55
    #
    def reduce(f)
      _reduce(Spark::Command::Reduce, f, f)
    end

    # Aggregate the elements of each partition, and then the results for all the partitions, using a
    # given associative function and a neutral "zero value".
    #
    # The function f(x, y) is allowed to modify x and return it as its result value to avoid
    # object allocation; however, it should not modify y.
    #
    # Be careful, zero_values is applied to all stages. See example.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10, 2)
    #   rdd.fold(1, lambda{|sum, x| sum+x})
    #   # => 58
    #
    def fold(zero_value, f)
      self.aggregate(zero_value, f, f)
    end

    # Aggregate the elements of each partition, and then the results for all the partitions, using
    # given combine functions and a neutral "zero value".
    #
    # This function can return a different result type. We need one operation for merging.
    #
    # Result must be an Array otherwise Serializer Array's zero value will be send
    # as multiple values and not just one.
    #
    # == Example:
    #   # 1 2 3 4 5  => 15 + 1 = 16
    #   # 6 7 8 9 10 => 40 + 1 = 41
    #   # 16 * 41 = 656
    #
    #   seq = lambda{|x,y| x+y}
    #   com = lambda{|x,y| x*y}
    #
    #   rdd = $sc.parallelize(1..10, 2)
    #   rdd.aggregate(1, seq, com)
    #   # => 656
    #
    def aggregate(zero_value, seq_op, comb_op)
      _reduce(Spark::Command::Aggregate, seq_op, comb_op, zero_value)
    end

    # Return the max of this RDD
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.max
    #   # => 10
    #
    def max
      self.reduce('lambda{|memo, item| memo > item ? memo : item }')
    end

    # Return the min of this RDD
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.min
    #   # => 0
    #
    def min
      self.reduce('lambda{|memo, item| memo < item ? memo : item }')
    end

    # Return the sum of this RDD
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.sum
    #   # => 55
    #
    def sum
      self.reduce('lambda{|sum, item| sum + item}')
    end

    # Return the number of values in this RDD
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.count
    #   # => 11
    #
    def count
      # nil is for seq_op => it means the all result go directly to one worker for combine
      @count ||= self.map_partitions('lambda{|iterator| iterator.to_a.size }')
                     .aggregate(0, nil, 'lambda{|sum, item| sum + item }')
    end

    # Return a {Spark::StatCounter} object that captures the mean, variance
    # and count of the RDD's elements in one operation.
    def stats
      @stats ||= new_rdd_from_command(Spark::Command::Stats).reduce('lambda{|memo, item| memo.merge(item)}')
    end

    # Compute the mean of this RDD's elements.
    #
    # == Example:
    #   $sc.parallelize([1, 2, 3]).mean
    #   # => 2.0
    #
    def mean
      stats.mean
    end

    # Compute the variance of this RDD's elements.
    #
    # == Example:
    #   $sc.parallelize([1, 2, 3]).variance
    #   # => 0.666...
    #
    def variance
      stats.variance
    end

    # Compute the standard deviation of this RDD's elements.
    #
    # == Example:
    #   $sc.parallelize([1, 2, 3]).stdev
    #   # => 0.816...
    #
    def stdev
      stats.stdev
    end

    # Compute the sample standard deviation of this RDD's elements (which
    # corrects for bias in estimating the standard deviation by dividing by
    # N-1 instead of N).
    #
    # == Example:
    #   $sc.parallelize([1, 2, 3]).sample_stdev
    #   # => 1.0
    #
    def sample_stdev
      stats.sample_stdev
    end

    # Compute the sample variance of this RDD's elements (which corrects
    # for bias in estimating the variance by dividing by N-1 instead of N).
    #
    # == Example:
    #   $sc.parallelize([1, 2, 3]).sample_variance
    #   # => 1.0
    #
    def sample_variance
      stats.sample_variance
    end

    # Compute a histogram using the provided buckets. The buckets
    # are all open to the right except for the last which is closed.
    # e.g. [1,10,20,50] means the buckets are [1,10) [10,20) [20,50],
    # which means 1<=x<10, 10<=x<20, 20<=x<=50. And on the input of 1
    # and 50 we would have a histogram of 1,0,1.
    #
    # If your histogram is evenly spaced (e.g. [0, 10, 20, 30]),
    # this can be switched from an O(log n) inseration to O(1) per
    # element(where n = # buckets).
    #
    # Buckets must be sorted and not contain any duplicates, must be
    # at least two elements.
    #
    # == Examples:
    #   rdd = $sc.parallelize(0..50)
    #
    #   rdd.histogram(2)
    #   # => [[0.0, 25.0, 50], [25, 26]]
    #
    #   rdd.histogram([0, 5, 25, 50])
    #   # => [[0, 5, 25, 50], [5, 20, 26]]
    #
    #   rdd.histogram([0, 15, 30, 45, 60])
    #   # => [[0, 15, 30, 45, 60], [15, 15, 15, 6]]
    #
    def histogram(buckets)

      # -----------------------------------------------------------------------
      # Integer
      #
      if buckets.is_a?(Integer)

        # Validation
        if buckets < 1
          raise ArgumentError, "Bucket count must be >= 1, #{buckets} inserted."
        end

        # Filter invalid values
        # Nil and NaN
        func = 'lambda{|x|
          if x.nil? || (x.is_a?(Float) && x.nan?)
            false
          else
            true
          end
        }'
        filtered = self.filter(func)

        # Compute the minimum and the maximum
        func = 'lambda{|memo, item|
          [memo[0] < item[0] ? memo[0] : item[0],
           memo[1] > item[1] ? memo[1] : item[1]]
        }'
        min, max = filtered.map('lambda{|x| [x, x]}').reduce(func)

        # Min, max must be valid numbers
        if (min.is_a?(Float) && !min.finite?) || (max.is_a?(Float) && !max.finite?)
          raise Spark::RDDError, 'Histogram on either an empty RDD or RDD containing +/-infinity or NaN'
        end

        # Already finished
        if min == max || buckets == 1
          return [min, max], [filtered.count]
        end

        # Custom range
        begin
          span = max - min # increment
          buckets = (0...buckets).map do |x|
            min + (x * span) / buckets.to_f
          end
          buckets << max
        rescue NoMethodError
          raise Spark::RDDError, 'Can not generate buckets with non-number in RDD'
        end

        even = true

      # -----------------------------------------------------------------------
      # Array
      #
      elsif buckets.is_a?(Array)

        if buckets.size < 2
          raise ArgumentError, 'Buckets should have more than one value.'
        end

        if buckets.detect{|x| x.nil? || (x.is_a?(Float) && x.nan?)}
          raise ArgumentError, 'Can not have nil or nan numbers in buckets.'
        end

        if buckets.detect{|x| buckets.count(x) > 1}
          raise ArgumentError, 'Buckets should not contain duplicated values.'
        end

        if buckets.sort != buckets
          raise ArgumentError, 'Buckets must be sorted.'
        end

        even = false

      # -----------------------------------------------------------------------
      # Other
      #
      else
        raise Spark::RDDError, 'Buckets should be number or array.'
      end

      reduce_func = 'lambda{|memo, item|
        memo.size.times do |i|
          memo[i] += item[i]
        end
        memo
      }'

      return buckets, new_rdd_from_command(Spark::Command::Histogram, even, buckets).reduce(reduce_func)
    end

    # Applies a function f to all elements of this RDD.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..5)
    #   rdd.foreach(lambda{|x| puts x})
    #   # => nil
    #
    def foreach(f, options={})
      new_rdd_from_command(Spark::Command::Foreach, f).collect
      nil
    end

    # Applies a function f to each partition of this RDD.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..5)
    #   rdd.foreachPartition(lambda{|x| puts x.to_s})
    #   # => nil
    #
    def foreach_partition(f, options={})
      new_rdd_from_command(Spark::Command::ForeachPartition, f).collect
      nil
    end


    # =============================================================================
    # Transformations of RDD

    # Return a new RDD by applying a function to all elements of this RDD.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..5)
    #   rdd.map(lambda {|x| x*2}).collect
    #   # => [0, 2, 4, 6, 8, 10]
    #
    def map(f)
      new_rdd_from_command(Spark::Command::Map, f)
    end

    # Return a new RDD by first applying a function to all elements of this
    # RDD, and then flattening the results.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..5)
    #   rdd.flat_map(lambda {|x| [x, 1]}).collect
    #   # => [0, 1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1]
    #
    def flat_map(f)
      new_rdd_from_command(Spark::Command::FlatMap, f)
    end

    # Return a new RDD by applying a function to each partition of this RDD.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10, 2)
    #   rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect
    #   # => [15, 40]
    #
    def map_partitions(f)
      new_rdd_from_command(Spark::Command::MapPartitions, f)
    end

    # Return a new RDD by applying a function to each partition of this RDD, while tracking the index
    # of the original partition.
    #
    # == Example:
    #   rdd = $sc.parallelize(0...4, 4)
    #   rdd.map_partitions_with_index(lambda{|part, index| part.first * index}).collect
    #   # => [0, 1, 4, 9]
    #
    def map_partitions_with_index(f, options={})
      new_rdd_from_command(Spark::Command::MapPartitionsWithIndex, f)
    end

    # Return a new RDD containing only the elements that satisfy a predicate.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.filter(lambda{|x| x.even?}).collect
    #   # => [0, 2, 4, 6, 8, 10]
    #
    def filter(f)
      new_rdd_from_command(Spark::Command::Filter, f)
    end

    # Return a new RDD containing non-nil elements.
    #
    # == Example:
    #   rdd = $sc.parallelize([1, nil, 2, nil, 3])
    #   rdd.compact.collect
    #   # => [1, 2, 3]
    #
    def compact
      new_rdd_from_command(Spark::Command::Compact)
    end

    # Return an RDD created by coalescing all elements within each partition into an array.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10, 3)
    #   rdd.glom.collect
    #   # => [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9, 10]]
    #
    def glom
      new_rdd_from_command(Spark::Command::Glom)
    end

    # Return a new RDD that is reduced into num_partitions partitions.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10, 3)
    #   rdd.coalesce(2).glom.collect
    #   # => [[0, 1, 2], [3, 4, 5, 6, 7, 8, 9, 10]]
    #
    def coalesce(num_partitions)
      if self.is_a?(PipelinedRDD)
        deser = @command.serializer
      else
        deser = @command.deserializer
      end

      new_jrdd = jrdd.coalesce(num_partitions)
      RDD.new(new_jrdd, context, @command.serializer, deser)
    end

    # Return the Cartesian product of this RDD and another one, that is, the
    # RDD of all pairs of elements `(a, b)` where `a` is in `self` and
    # `b` is in `other`.
    #
    # == Example:
    #   rdd1 = $sc.parallelize([1,2,3])
    #   rdd2 = $sc.parallelize([4,5,6])
    #
    #   rdd1.cartesian(rdd2).collect
    #   # => [[1, 4], [1, 5], [1, 6], [2, 4], [2, 5], [2, 6], [3, 4], [3, 5], [3, 6]]
    #
    def cartesian(other)
      _deserializer = Spark::Serializer::Cartesian.new(self.deserializer, other.deserializer)

      new_jrdd = jrdd.cartesian(other.jrdd)
      RDD.new(new_jrdd, context, serializer, _deserializer)
    end

    # Return a new RDD containing the distinct elements in this RDD.
    # Ordering is not preserved because of reducing
    #
    # == Example:
    #   rdd = $sc.parallelize([1,1,1,2,3])
    #   rdd.distinct.collect
    #   # => [1, 2, 3]
    #
    def distinct
      self.map('lambda{|x| [x, nil]}')
          .reduce_by_key('lambda{|x,_| x}')
          .map('lambda{|x| x[0]}')
    end

    # Return a shuffled RDD.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd.shuffle.collect
    #   # => [3, 10, 6, 7, 8, 0, 4, 2, 9, 1, 5]
    #
    def shuffle(seed=nil)
      seed ||= Random.new_seed

      new_rdd_from_command(Spark::Command::Shuffle, seed)
    end

    # Return the union of this RDD and another one. Any identical elements will appear multiple
    # times (use .distinct to eliminate them).
    #
    # == Example:
    #   rdd = $sc.parallelize([1, 2, 3])
    #   rdd.union(rdd).collect
    #   # => [1, 2, 3, 1, 2, 3]
    #
    def union(other)
      if self.serializer != other.serializer
        other = other.reserialize(serializer)
      end

      new_jrdd = jrdd.union(other.jrdd)
      RDD.new(new_jrdd, context, serializer, deserializer)
    end

    # Return a new RDD with different serializer. This method is useful during union
    # and join operations.
    #
    # == Example:
    #   rdd = $sc.parallelize([1, 2, 3], nil, serializer: "marshal")
    #   rdd = rdd.map(lambda{|x| x.to_s})
    #   rdd.reserialize("oj").collect
    #   # => ["1", "2", "3"]
    #
    def reserialize(new_serializer)
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
    # == Example:
    #   rdd1 = $sc.parallelize([1,2,3,4,5])
    #   rdd2 = $sc.parallelize([1,4,5,6,7])
    #   rdd1.intersection(rdd2).collect
    #   # => [1, 4, 5]
    #
    def intersection(other)
      mapping_function = 'lambda{|item| [item, nil]}'
      filter_function  = 'lambda{|(key, values)| values.size > 1}'

      self.map(mapping_function)
          .cogroup(other.map(mapping_function))
          .filter(filter_function)
          .keys
    end

    # Return a copy of the RDD partitioned using the specified partitioner.
    #
    # == Example:
    #   rdd = $sc.parallelize(["1","2","3","4","5"]).map(lambda {|x| [x, 1]})
    #   rdd.partitionBy(2).glom.collect
    #   # => [[["3", 1], ["4", 1]], [["1", 1], ["2", 1], ["5", 1]]]
    #
    def partition_by(num_partitions, partition_func=nil)
      num_partitions ||= default_reduce_partitions
      partition_func ||= 'lambda{|x| Spark::Digest.portable_hash(x.to_s)}'

      _partition_by(num_partitions, Spark::Command::PartitionBy::Basic, partition_func)
    end

    # Return a sampled subset of this RDD. Operations are base on Poisson and Uniform
    # distributions.
    # TODO: Replace Unfirom for Bernoulli
    #
    # == Examples:
    #   rdd = $sc.parallelize(0..100)
    #
    #   rdd.sample(true, 10).collect
    #   # => [17, 17, 22, 23, 51, 52, 62, 64, 69, 70, 96]
    #
    #   rdd.sample(false, 0.1).collect
    #   # => [3, 5, 9, 32, 44, 55, 66, 68, 75, 80, 86, 91, 98]
    #
    def sample(with_replacement, fraction, seed=nil)
      new_rdd_from_command(Spark::Command::Sample, with_replacement, fraction, seed)
    end

    # Return a fixed-size sampled subset of this RDD in an array
    #
    # == Examples:
    #   rdd = $sc.parallelize(0..100)
    #
    #   rdd.take_sample(true, 10)
    #   # => [90, 84, 74, 44, 27, 22, 72, 96, 80, 54]
    #
    #   rdd.take_sample(false, 10)
    #   # => [5, 35, 30, 48, 22, 33, 40, 75, 42, 32]
    #
    def take_sample(with_replacement, num, seed=nil)

      if num < 0
        raise Spark::RDDError, 'Size have to be greater than 0'
      elsif num == 0
        return []
      end

      # Taken from scala
      num_st_dev = 10.0

      # Number of items
      initial_count = self.count
      return [] if initial_count == 0

      # Create new generator
      seed ||= Random.new_seed
      rng = Random.new(seed)

      # Shuffle elements if requested num if greater than array size
      if !with_replacement && num >= initial_count
        return self.shuffle(seed).collect
      end

      # Max num
      max_sample_size = Integer::MAX - (num_st_dev * Math.sqrt(Integer::MAX)).to_i
      if num > max_sample_size
        raise Spark::RDDError, "Size can not be greate than #{max_sample_size}"
      end

      # Approximate fraction with tolerance
      fraction = compute_fraction(num, initial_count, with_replacement)

      # Compute first samled subset
      samples = self.sample(with_replacement, fraction, seed).collect

      # If the first sample didn't turn out large enough, keep trying to take samples;
      # this shouldn't happen often because we use a big multiplier for their initial size.
      index = 0
      while samples.size < num
        log_warning("Needed to re-sample due to insufficient sample size. Repeat #{index}")
        samples = self.sample(with_replacement, fraction, rng.rand(0..Integer::MAX)).collect
        index += 1
      end

      samples.shuffle!(random: rng)
      samples[0, num]
    end

    # Return an RDD created by piping elements to a forked external process.
    #
    # == Cmds:
    #   cmd = [env,] command... [,options]
    #
    #   env: hash
    #     name => val : set the environment variable
    #     name => nil : unset the environment variable
    #   command...:
    #     commandline                 : command line string which is passed to the standard shell
    #     cmdname, arg1, ...          : command name and one or more arguments (This form does
    #                                   not use the shell. See below for caveats.)
    #     [cmdname, argv0], arg1, ... : command name, argv[0] and zero or more arguments (no shell)
    #   options: hash
    #
    #   See http://ruby-doc.org/core-2.2.0/Process.html#method-c-spawn
    #
    # == Examples:
    #   $sc.parallelize(0..5).pipe('cat').collect
    #   # => ["0", "1", "2", "3", "4", "5"]
    #
    #   rdd = $sc.parallelize(0..5)
    #   rdd = rdd.pipe('cat', "awk '{print $1*10}'")
    #   rdd = rdd.map(lambda{|x| x.to_i + 1})
    #   rdd.collect
    #   # => [1, 11, 21, 31, 41, 51]
    #
    def pipe(*cmds)
      new_rdd_from_command(Spark::Command::Pipe, cmds)
    end


    # =============================================================================
    # Pair functions

    # Merge the values for each key using an associative reduce function. This will also perform
    # the merging locally on each mapper before sending results to a reducer, similarly to a
    # "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
    # parallelism level.
    #
    # == Example:
    #   rdd = $sc.parallelize(["a","b","c","a","b","c","a","c"]).map(lambda{|x| [x, 1]})
    #   rdd.reduce_by_key(lambda{|x,y| x+y}).collect_as_hash
    #   # => {"a"=>3, "b"=>2, "c"=>3}
    #
    def reduce_by_key(f, num_partitions=nil)
      combine_by_key('lambda {|x| x}', f, f, num_partitions)
    end

    # Generic function to combine the elements for each key using a custom set of aggregation
    # functions. Turns a JavaPairRDD[(K, V)] into a result of type JavaPairRDD[(K, C)], for a
    # "combined type" C * Note that V and C can be different -- for example, one might group an
    # RDD of type (Int, Int) into an RDD of type (Int, List[Int]). Users provide three
    # functions:
    #
    # == Parameters:
    # create_combiner:: which turns a V into a C (e.g., creates a one-element list)
    # merge_value:: to merge a V into a C (e.g., adds it to the end of a list)
    # merge_combiners:: to combine two C's into a single one.
    #
    # == Example:
    #   def combiner(x)
    #     x
    #   end
    #
    #   def merge(x,y)
    #     x+y
    #   end
    #
    #   rdd = $sc.parallelize(["a","b","c","a","b","c","a","c"], 2).map(lambda{|x| [x, 1]})
    #   rdd.combine_by_key(method(:combiner), method(:merge), method(:merge)).collect_as_hash
    #   # => {"a"=>3, "b"=>2, "c"=>3}
    #
    def combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions=nil)
      _combine_by_key(
        [Spark::Command::CombineByKey::Combine, create_combiner, merge_value],
        [Spark::Command::CombineByKey::Merge, merge_combiners],
        num_partitions
      )
    end

    # Return an RDD of grouped items.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..5)
    #   rdd.group_by(lambda{|x| x%2}).collect
    #   # => [[0, [0, 2, 4]], [1, [1, 3, 5]]]
    #
    def group_by(f, num_partitions=nil)
      self.key_by(f).group_by_key(num_partitions)
    end

    # Group the values for each key in the RDD into a single sequence. Allows controlling the
    # partitioning of the resulting key-value pair RDD by passing a Partitioner.
    #
    # Note: If you are grouping in order to perform an aggregation (such as a sum or average)
    # over each key, using reduce_by_key or combine_by_key will provide much better performance.
    #
    # == Example:
    #   rdd = $sc.parallelize([["a", 1], ["a", 2], ["b", 3]])
    #   rdd.group_by_key.collect
    #   # => [["a", [1, 2]], ["b", [3]]]
    #
    def group_by_key(num_partitions=nil)
      create_combiner = 'lambda{|item| [item]}'
      merge_value     = 'lambda{|combiner, item| combiner << item; combiner}'
      merge_combiners = 'lambda{|combiner_1, combiner_2| combiner_1 += combiner_2; combiner_1}'

      combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions)
    end

    # Merge the values for each key using an associative function f
    # and a neutral `zero_value` which may be added to the result an
    # arbitrary number of times, and must not change the result
    # (e.g., 0 for addition, or 1 for multiplication.).
    #
    # == Example:
    #   rdd = $sc.parallelize([["a", 1], ["b", 2], ["a", 3], ["a", 4], ["c", 5]])
    #   rdd.fold_by_key(1, lambda{|x,y| x+y})
    #   # => [["a", 9], ["c", 6], ["b", 3]]
    #
    def fold_by_key(zero_value, f, num_partitions=nil)
      self.aggregate_by_key(zero_value, f, f, num_partitions)
    end

    # Aggregate the values of each key, using given combine functions and a neutral zero value.
    #
    # == Example:
    #   def combine(x,y)
    #     x+y
    #   end
    #
    #   def merge(x,y)
    #     x*y
    #   end
    #
    #   rdd = $sc.parallelize([["a", 1], ["b", 2], ["a", 3], ["a", 4], ["c", 5]], 2)
    #   rdd.aggregate_by_key(1, method(:combine), method(:merge))
    #   # => [["b", 3], ["a", 16], ["c", 6]]
    #
    def aggregate_by_key(zero_value, seq_func, comb_func, num_partitions=nil)
      _combine_by_key(
        [Spark::Command::CombineByKey::CombineWithZero, zero_value, seq_func],
        [Spark::Command::CombineByKey::Merge, comb_func],
        num_partitions
      )
    end

    # The same functionality as cogroup but this can grouped only 2 rdd's and you
    # can change num_partitions.
    #
    # == Example:
    #   rdd1 = $sc.parallelize([["a", 1], ["a", 2], ["b", 3]])
    #   rdd2 = $sc.parallelize([["a", 4], ["a", 5], ["b", 6]])
    #   rdd1.group_with(rdd2).collect
    #   # => [["a", [1, 2, 4, 5]], ["b", [3, 6]]]
    #
    def group_with(other, num_partitions=nil)
      self.union(other).group_by_key(num_partitions)
    end

    # For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
    # list of values for that key in `this` as well as `other`.
    #
    # == Example:
    #   rdd1 = $sc.parallelize([["a", 1], ["a", 2], ["b", 3]])
    #   rdd2 = $sc.parallelize([["a", 4], ["a", 5], ["b", 6]])
    #   rdd3 = $sc.parallelize([["a", 7], ["a", 8], ["b", 9]])
    #   rdd1.cogroup(rdd2, rdd3).collect
    #   # => [["a", [1, 2, 4, 5, 7, 8]], ["b", [3, 6, 9]]]
    #
    def cogroup(*others)
      unioned = self
      others.each do |other|
        unioned = unioned.union(other)
      end

      unioned.group_by_key
    end

    # Return each (key, value) pair in self RDD that has no pair with matching
    # key in other RDD.
    #
    # == Example:
    #   rdd1 = $sc.parallelize([["a", 1], ["a", 2], ["b", 3], ["c", 4]])
    #   rdd2 = $sc.parallelize([["b", 5], ["c", 6]])
    #   rdd1.subtract_by_key(rdd2).collect
    #   # => [["a", 1], ["a", 2]]
    #
    def subtract_by_key(other, num_partitions=nil)
      create_combiner = 'lambda{|item| [[item]]}'
      merge_value     = 'lambda{|combiner, item| combiner.first << item; combiner}'
      merge_combiners = 'lambda{|combiner_1, combiner_2| combiner_1 += combiner_2; combiner_1}'

      self.union(other)
          .combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions)
          .filter('lambda{|(key,values)| values.size == 1}')
          .flat_map_values('lambda{|item| item.first}')
    end

    # Return an RDD with the elements from self that are not in other.
    #
    # == Example:
    #   rdd1 = $sc.parallelize([["a", 1], ["a", 2], ["b", 3], ["c", 4]])
    #   rdd2 = $sc.parallelize([["a", 2], ["c", 6]])
    #   rdd1.subtract(rdd2).collect
    #   # => [["a", 1], ["b", 3], ["c", 4]]
    #
    def subtract(other, num_partitions=nil)
      mapping_function = 'lambda{|x| [x,nil]}'

      self.map(mapping_function)
          .subtract_by_key(other.map(mapping_function), num_partitions)
          .keys
    end

    # Sort the RDD by key
    #
    # == Example:
    #   rdd = $sc.parallelize([["c", 1], ["b", 2], ["a", 3]])
    #   rdd.sort_by_key.collect
    #   # => [["a", 3], ["b", 2], ["c", 1]]
    #
    def sort_by_key(ascending=true, num_partitions=nil)
      self.sort_by('lambda{|(key, _)| key}')
    end

    # Sort the RDD by value
    #
    # == Example:
    #   rdd = $sc.parallelize([["a", 3], ["b", 1], ["c", 2]])
    #   rdd.sort_by_value.collect
    #   # => [["b", 1], ["c", 2], ["a", 3]]
    #
    def sort_by_value(ascending=true, num_partitions=nil)
      self.sort_by('lambda{|(_, value)| value}')
    end

    # Sorts this RDD by the given key_function
    #
    # This is a different implementation than spark. Sort by doesn't use
    # key_by method first. It can be slower but take less memory and
    # you can always use map.sort_by_key
    #
    # == Example:
    #   rdd = $sc.parallelize(["aaaaaaa", "cc", "b", "eeee", "ddd"])
    #
    #   rdd.sort_by.collect
    #   # => ["aaaaaaa", "b", "cc", "ddd", "eeee"]
    #
    #   rdd.sort_by(lambda{|x| x.size}).collect
    #   # => ["b", "cc", "ddd", "eeee", "aaaaaaa"]
    #
    def sort_by(key_function=nil, ascending=true, num_partitions=nil)
      key_function   ||= 'lambda{|x| x}'
      num_partitions ||= default_reduce_partitions

      command_klass = Spark::Command::SortByKey

      # Allow spill data to disk due to memory limit
      # spilling = config['spark.shuffle.spill'] || false
      spilling = false
      memory = ''

      # Set spilling to false if worker has unlimited memory
      if memory.empty?
        spilling = false
        memory   = nil
      else
        memory = to_memory_size(memory)
      end

      # Sorting should do one worker
      if num_partitions == 1
        rdd = self
        rdd = rdd.coalesce(1) if partitions_size > 1
        return rdd.new_rdd_from_command(command_klass, key_function, ascending, spilling, memory, serializer)
      end

      # Compute boundary of collection
      # Collection should be evenly distributed
      # 20.0 is from scala RangePartitioner (for roughly balanced output partitions)
      count = self.count
      sample_size = num_partitions * 20.0
      fraction = [sample_size / [count, 1].max, 1.0].min
      samples = self.sample(false, fraction, 1).map(key_function).collect
      samples.sort!
      # Reverse is much faster than reverse sort_by
      samples.reverse! if !ascending

      # Determine part bounds
      bounds = determine_bounds(samples, num_partitions)

      shuffled = _partition_by(num_partitions, Spark::Command::PartitionBy::Sorting, key_function, bounds, ascending, num_partitions)
      shuffled.new_rdd_from_command(command_klass, key_function, ascending, spilling, memory, serializer)
    end

    # Creates array of the elements in this RDD by applying function f.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..5)
    #   rdd.key_by(lambda{|x| x%2}).collect
    #   # => [[0, 0], [1, 1], [0, 2], [1, 3], [0, 4], [1, 5]]
    #
    def key_by(f)
      new_rdd_from_command(Spark::Command::KeyBy, f)
    end

    # Pass each value in the key-value pair RDD through a map function without changing
    # the keys. This also retains the original RDD's partitioning.
    #
    # == Example:
    #   rdd = $sc.parallelize(["ruby", "scala", "java"])
    #   rdd = rdd.map(lambda{|x| [x, x]})
    #   rdd = rdd.map_values(lambda{|x| x.upcase})
    #   rdd.collect
    #   # => [["ruby", "RUBY"], ["scala", "SCALA"], ["java", "JAVA"]]
    #
    def map_values(f)
      new_rdd_from_command(Spark::Command::MapValues, f)
    end

    # Pass each value in the key-value pair RDD through a flat_map function
    # without changing the keys; this also retains the original RDD's
    # partitioning.
    #
    # == Example:
    #   rdd = $sc.parallelize([["a", [1,2]], ["b", [3]]])
    #   rdd = rdd.flat_map_values(lambda{|x| x*2})
    #   rdd.collect
    #   # => [["a", 1], ["a", 2], ["a", 1], ["a", 2], ["b", 3], ["b", 3]]
    #
    def flat_map_values(f)
      new_rdd_from_command(Spark::Command::FlatMapValues, f)
    end

    # Return an RDD with the first element of PairRDD
    #
    # == Example:
    #   rdd = $sc.parallelize([[1,2], [3,4], [5,6]])
    #   rdd.keys.collect
    #   # => [1, 3, 5]
    #
    def keys
      self.map('lambda{|(key, _)| key}')
    end

    # Return an RDD with the second element of PairRDD
    #
    # == Example:
    #   rdd = $sc.parallelize([[1,2], [3,4], [5,6]])
    #   rdd.keys.collect
    #   # => [2, 4, 6]
    #
    def values
      self.map('lambda{|(_, value)| value}')
    end

    # Return the list of values in the RDD for key `key`.
    # TODO: add Partitioner for efficiently searching
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10)
    #   rdd = rdd.group_by(lambda {|x| x%3})
    #   rdd.lookup(2)
    #   # => [[2, 5, 8]]
    #
    #   rdd = $sc.parallelize(0..10)
    #   rdd = rdd.key_by(lambda{|x| x.even?})
    #   rdd.lookup(true)
    #   # => [0, 2, 4, 6, 8, 10]
    #
    def lookup(key)
      lookup_key = "lookup_key_#{object_id}"

      self.filter("lambda{|(key, _)| key == #{lookup_key}}")
          .bind(lookup_key => key)
          .values
          .collect
    end

    # Aliases
    alias_method :partitionsSize, :partitions_size
    alias_method :defaultReducePartitions, :default_reduce_partitions
    alias_method :setName, :set_name
    alias_method :addLibrary, :add_library
    alias_method :require, :add_library

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
    alias_method :takeSample, :take_sample
    alias_method :sortBy, :sort_by
    alias_method :sortByKey, :sort_by_key
    alias_method :keyBy, :key_by
    alias_method :groupBy, :group_by
    alias_method :foldByKey, :fold_by_key
    alias_method :aggregateByKey, :aggregate_by_key
    alias_method :subtractByKey, :subtract_by_key
    alias_method :sampleStdev, :sample_stdev
    alias_method :sampleVariance, :sample_variance

    private

      # This is base method for reduce operation. Is used by reduce, fold and aggregation.
      # Only difference is that fold has zero value.
      #
      def _reduce(klass, seq_op, comb_op, zero_value=nil)
        if seq_op.nil?
          # Partitions are already reduced
          rdd = self
        else
          rdd = new_rdd_from_command(klass, seq_op, zero_value)
        end

        # Send all results to one worker and combine results
        rdd = rdd.coalesce(1).compact

        # Add the same function to new RDD
        comm = rdd.add_command(klass, comb_op, zero_value)
        comm.deserializer = @command.serializer

        # Value is returned in array
        PipelinedRDD.new(rdd, comm).collect[0]
      end

      def _partition_by(num_partitions, klass, *args)
        # RDD is transform from [key, value] to [hash, [key, value]]
        keyed = new_rdd_from_command(klass, *args)
        keyed.serializer.unbatch!

        # PairwiseRDD and PythonPartitioner are borrowed from Python
        # but works great on ruby too
        pairwise_rdd = PairwiseRDD.new(keyed.jrdd.rdd).asJavaPairRDD
        partitioner = PythonPartitioner.new(num_partitions, args.first.object_id)
        new_jrdd = pairwise_rdd.partitionBy(partitioner).values

        # Reset deserializer
        RDD.new(new_jrdd, context, @command.serializer, keyed.serializer)
      end

      # For using a different combine_by_key
      #
      # == Used for:
      # * combine_by_key
      # * fold_by_key (with zero value)
      #
      def _combine_by_key(combine, merge, num_partitions)
        num_partitions ||= default_reduce_partitions

        # Combine key
        combined = new_rdd_from_command(combine.shift, *combine)

        # Merge items
        shuffled = combined.partition_by(num_partitions)
        merge_comm = shuffled.add_command(merge.shift, *merge)

        PipelinedRDD.new(shuffled, merge_comm)
      end

  end

  # Pipelined Resilient Distributed Dataset, operations are pipelined and sended to worker
  #
  #   RDD
  #   `-- map
  #       `-- map
  #           `-- map
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

        broadcasts = @command.bound_objects.select{|_, value| value.is_a?(Spark::Broadcast)}.values
        broadcasts = to_java_array_list(broadcasts.map(&:jbroadcast))

        ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, broadcasts, @context.jaccumulator)
        ruby_rdd.asJavaRDD
      end

  end
end
