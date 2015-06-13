# Necessary libraries
Spark.load_lib

module Spark
  ##
  # Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
  # cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
  #
  class Context

    include Spark::Helper::System
    include Spark::Helper::Parser
    include Spark::Helper::Logger

    attr_reader :jcontext, :jaccumulator, :temp_dir

    # Constructor for Ruby context. Configuration is automatically is taken
    # from Spark. Config will be automatically set to default if user start
    # context first.
    #
    def initialize
      Spark.config.valid!
      @jcontext = JavaSparkContext.new(Spark.config.spark_conf)
      @jcontext.addJar(Spark.ruby_spark_jar)

      # Does not work on 1.2
      # ui.attachTab(RubyTab.new(ui, to_java_hash(RbConfig::CONFIG)))

      spark_local_dir = JUtils.getLocalDir(sc.conf)
      @temp_dir = JUtils.createTempDir(spark_local_dir, 'ruby').getAbsolutePath

      accum_server = Spark::Accumulator::Server
      accum_server.start
      @jaccumulator = @jcontext.accumulator(ArrayList.new, RubyAccumulatorParam.new(accum_server.host, accum_server.port))

      log_info("Ruby accumulator server is running on port #{accum_server.port}")

      set_call_site('Ruby') # description of stage
    end

    def inspect
      result  = %{#<#{self.class.name}:0x#{object_id}\n}
      result << %{Tempdir: "#{temp_dir}">}
      result
    end

    def stop
      Spark::Accumulator::Server.stop
      log_info('Ruby accumulator server was stopped')
      @jcontext.stop
    end

    def sc
      @jcontext.sc
    end

    def ui
      sc.ui
    end

    # Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD)
    #
    def default_parallelism
      sc.defaultParallelism
    end

    # Default serializer
    #
    # Batch -> Compress -> Basic
    #
    def default_serializer
      # Basic
      serializer = Spark::Serializer.find!(config('spark.ruby.serializer')).new

      # Compress
      if config('spark.ruby.serializer.compress')
        serializer = Spark::Serializer.compressed(serializer)
      end

      # Bactching
      batch_size = default_batch_size
      if batch_size == 'auto'
        serializer = Spark::Serializer.auto_batched(serializer)
      else
        serializer = Spark::Serializer.batched(serializer, batch_size)
      end

      # Finally, "container" contains serializers
      serializer
    end

    def default_batch_size
      size = config('spark.ruby.serializer.batch_size').to_i
      if size >= 1
        size
      else
        'auto'
      end
    end

    # Set a local property that affects jobs submitted from this thread, such as the
    # Spark fair scheduler pool.
    #
    def set_local_property(key, value)
      jcontext.setLocalProperty(key, value)
    end

    # Get a local property set in this thread, or null if it is missing
    #
    def get_local_property(key)
      jcontext.getLocalProperty(key)
    end

    # Support function for API backtraces.
    #
    def set_call_site(site)
      jcontext.setCallSite(site)
    end

    def clear_call_site
      jcontext.clearCallSite
    end

    # Return a copy of this SparkContext's configuration. The configuration *cannot*
    # be changed at runtime.
    #
    def config(key=nil)
      if key
        Spark.config.get(key)
      else
        Spark.config
      end
    end

    # Add a file to be downloaded with this Spark job on every node.
    # The path of file passed can be either a local file, a file in HDFS
    # (or other Hadoop-supported filesystems), or an HTTP, HTTPS or FTP URI.
    #
    # To access the file in Spark jobs, use `SparkFiles.get(file_name)` with the
    # filename to find its download location.
    #
    # == Example:
    #   `echo 10 > test.txt`
    #
    #   $sc.add_file('test.txt')
    #   $sc.parallelize(0..5).map(lambda{|x| x * SparkFiles.get_content('test.txt').to_i}).collect
    #   # => [0, 10, 20, 30, 40, 50]
    #
    def add_file(*files)
      files.each do |file|
        sc.addFile(file)
      end
    end

    # Broadcast a read-only variable to the cluster, returning a Spark::Broadcast
    # object for reading it in distributed functions. The variable will
    # be sent to each cluster only once.
    #
    # == Example:
    #   broadcast1 = $sc.broadcast('a')
    #   broadcast2 = $sc.broadcast('b')
    #
    #   rdd = $sc.parallelize(0..5, 4)
    #   rdd = rdd.bind(broadcast1: broadcast1, broadcast2: broadcast2)
    #   rdd = rdd.map_partitions_with_index(lambda{|part, index| [broadcast1.value * index, broadcast2.value * index] })
    #   rdd.collect
    #   # => ["", "", "a", "b", "aa", "bb", "aaa", "bbb"]
    #
    def broadcast(value)
      Spark::Broadcast.new(self, value)
    end

    # Create an Accumulator with the given initial value, using a given
    # accum_param helper object to define how to add values of the
    # data type if provided.
    #
    # == Example:
    #   accum = $sc.accumulator(7)
    #
    #   rdd = $sc.parallelize(0..5, 4)
    #   rdd = rdd.bind(accum: accum)
    #   rdd = rdd.map_partitions(lambda{|_| accum.add(1) })
    #   rdd = rdd.collect
    #
    #   accum.value
    #   # => 11
    #
    def accumulator(value, accum_param=:+, zero_value=0)
      Spark::Accumulator.new(value, accum_param, zero_value)
    end

    # Distribute a local Ruby collection to form an RDD
    # Direct method can be slow so be careful, this method update data inplace
    #
    # == Parameters:
    # data:: Range or Array
    # num_slices:: number of slice
    # serializer:: custom serializer (default: serializer based on configuration)
    #
    # == Examples:
    #   $sc.parallelize(["1", "2", "3"]).map(lambda{|x| x.to_i}).collect
    #   #=> [1, 2, 3]
    #
    #   $sc.parallelize(1..3).map(:to_s).collect
    #   #=> ["1", "2", "3"]
    #
    def parallelize(data, num_slices=nil, serializer=nil)
      num_slices ||= default_parallelism
      serializer ||= default_serializer

      serializer.check_each(data)

      # Through file
      file = Tempfile.new('to_parallelize', temp_dir)
      serializer.dump_to_io(data, file)
      file.close # not unlink
      jrdd = RubyRDD.readRDDFromFile(jcontext, file.path, num_slices)

      Spark::RDD.new(jrdd, self, serializer)
    ensure
      file && file.unlink
    end

    # Read a text file from HDFS, a local file system (available on all nodes), or any
    # Hadoop-supported file system URI, and return it as an RDD of Strings.
    #
    # == Example:
    #   f = Tempfile.new("test")
    #   f.puts("1")
    #   f.puts("2")
    #   f.close
    #
    #   $sc.text_file(f.path).map(lambda{|x| x.to_i}).collect
    #   # => [1, 2]
    #
    def text_file(path, min_partitions=nil, encoding=Encoding::UTF_8, serializer=nil)
      min_partitions ||= default_parallelism
      serializer     ||= default_serializer
      deserializer     = Spark::Serializer.build { __text__(encoding) }

      Spark::RDD.new(@jcontext.textFile(path, min_partitions), self, serializer, deserializer)
    end

    # Read a directory of text files from HDFS, a local file system (available on all nodes), or any
    # Hadoop-supported file system URI. Each file is read as a single record and returned in a
    # key-value pair, where the key is the path of each file, the value is the content of each file.
    #
    # == Example:
    #   dir = Dir.mktmpdir
    #   f1 = Tempfile.new("test1", dir)
    #   f2 = Tempfile.new("test2", dir)
    #   f1.puts("1"); f1.puts("2");
    #   f2.puts("3"); f2.puts("4");
    #   f1.close
    #   f2.close
    #
    #   $sc.whole_text_files(dir).flat_map(lambda{|key, value| value.split}).collect
    #   # => ["1", "2", "3", "4"]
    #
    def whole_text_files(path, min_partitions=nil, serializer=nil)
      min_partitions ||= default_parallelism
      serializer     ||= default_serializer
      deserializer     = Spark::Serializer.build{ __pair__(__text__, __text__) }

      Spark::RDD.new(@jcontext.wholeTextFiles(path, min_partitions), self, serializer, deserializer)
    end

    # Executes the given partition function f on the specified set of partitions,
    # returning the result as an array of elements.
    #
    # If partitions is not specified, this will run over all partitions.
    #
    # == Example:
    #   rdd = $sc.parallelize(0..10, 5)
    #   $sc.run_job(rdd, lambda{|x| x.to_s}, [0,2])
    #   # => ["[0, 1]", "[4, 5]"]
    #
    def run_job(rdd, f, partitions=nil, allow_local=false)
      run_job_with_command(rdd, partitions, allow_local, Spark::Command::MapPartitions, f)
    end

    # Execute the given command on specific set of partitions.
    #
    def run_job_with_command(rdd, partitions, allow_local, command, *args)
      if !partitions.nil? && !partitions.is_a?(Array)
        raise Spark::ContextError, 'Partitions must be nil or Array'
      end

      partitions_size = rdd.partitions_size

      # Execute all parts
      if partitions.nil?
        partitions = (0...partitions_size).to_a
      end

      # Can happend when you use coalesce
      partitions.delete_if {|part| part >= partitions_size}

      # Rjb represent Fixnum as Integer but Jruby as Long
      partitions = to_java_array_list(convert_to_java_int(partitions))

      # File for result
      file = Tempfile.new('collect', temp_dir)

      mapped = rdd.new_rdd_from_command(command, *args)
      RubyRDD.runJob(rdd.context.sc, mapped.jrdd, partitions, allow_local, file.path)

      mapped.collect_from_file(file)
    end


    # Aliases
    alias_method :textFile, :text_file
    alias_method :wholeTextFiles, :whole_text_files
    alias_method :defaultParallelism, :default_parallelism
    alias_method :setLocalProperty, :set_local_property
    alias_method :getLocalProperty, :get_local_property
    alias_method :setCallSite, :set_call_site
    alias_method :clearCallSite, :clear_call_site
    alias_method :runJob, :run_job
    alias_method :runJobWithCommand, :run_job_with_command
    alias_method :addFile, :add_file

  end
end
