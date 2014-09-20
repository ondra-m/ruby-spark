require "tempfile"

# Necessary libraries
Spark.load_lib

# Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
# cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
#
module Spark
  class Context

    include Spark::Helper::Platform

    attr_reader :jcontext

    # Constructor for Ruby context. Configuration is automatically is taken
    # from Spark. Config will be automatically set to default if user start
    # context first.
    #
    def initialize
      Spark.config.valid!
      @jcontext = JavaSparkContext.new(Spark.config.spark_conf)

      ui.attachTab(RubyTab.new(ui, config_for_java))

      set_call_site("Ruby") # description of stage
    end

    def stop
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
      @jcontext.sc.defaultParallelism
    end

    def get_serializer(serializer, *args)
      serializer   = Spark::Serializer.get(serializer)
      serializer ||= Spark::Serializer.get(config("spark.ruby.serializer"))
      serializer.new(config("spark.ruby.batch_size")).set(*args)
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
      set_local_property("externalCallSite", site)
    end

    # Capture the current user callsite and return a formatted version for printing. If the user
    # has overridden the call site, this will return the user's version.
    #
    def get_call_site
      jcontext.getCallSite
    end

    # Return a copy of this SparkContext's configuration. The configuration *cannot*
    # be changed at runtime.
    #
    def config(key=nil)
      if key
        Spark.config[key]
      else
        Spark.config.get_all
      end
    end

    # Distribute a local Ruby collection to form an RDD
    # Direct method can be slow so be careful, this method update data inplace
    #
    #   data: Range or Array
    #   num_slices: number of slice
    #   options: use
    #            serializer
    #            batch_size
    #
    # $sc.parallelize(["1", "2", "3"]).map(lambda{|x| x.to_i}).collect
    # => [1, 2, 3]
    #
    def parallelize(data, num_slices=nil, options={})
      num_slices ||= default_parallelism

      use = jruby? ? (options[:use] || :direct) : :file
      serializer = get_serializer(options[:serializer], options[:batch_size])

      if data.is_a?(Array) && config("spark.ruby.parallelize_strategy") == "deep_copy"
        data = data.deep_copy
      else
        # For enumerator or range
        data = data.to_a
      end

      case use
      when :direct
        serializer.dump_to_java(data)
        jrdd = jcontext.parallelize(data, num_slices)
      when :file
        file = Tempfile.new("to_parallelize")
        serializer.dump(data, file)
        file.close # not unlink
        jrdd = RubyRDD.readRDDFromFile(jcontext, file.path, num_slices)
        file.unlink
      end

      Spark::RDD.new(jrdd, self, serializer)
    end

    # Read a text file from HDFS, a local file system (available on all nodes), or any
    # Hadoop-supported file system URI, and return it as an RDD of Strings.
    #
    # f = Tempfile.new("test")
    # f.puts("1")
    # f.puts("2")
    # f.close
    #
    # $sc.text_file(f.path).map(lambda{|x| x.to_i}).collect
    # => [1, 2]
    #
    def text_file(path, min_partitions=nil, options={})
      min_partitions ||= default_parallelism
      serializer = get_serializer(options[:serializer], options[:batch_size])

      Spark::RDD.new(@jcontext.textFile(path, min_partitions), self, serializer, get_serializer("UTF8"))
    end

    # Read a directory of text files from HDFS, a local file system (available on all nodes), or any
    # Hadoop-supported file system URI. Each file is read as a single record and returned in a
    # key-value pair, where the key is the path of each file, the value is the content of each file.
    #
    # dir = Dir.mktmpdir
    # f1 = Tempfile.new("test1", dir)
    # f2 = Tempfile.new("test2", dir)
    # f1.puts("1"); f1.puts("2");
    # f2.puts("3"); f2.puts("4");
    # f1.close
    # f2.close
    #
    # $sc.whole_text_files(dir).flat_map(lambda{|key, value| value.split}).collect
    # => ["1", "2", "3", "4"]
    #
    def whole_text_files(path, min_partitions=nil, options={})
      min_partitions ||= default_parallelism
      serializer = get_serializer(options[:serializer], options[:batch_size])
      deserializer = get_serializer("Pair", get_serializer("UTF8"), get_serializer("UTF8"))

      Spark::RDD.new(@jcontext.wholeTextFiles(path, min_partitions), self, serializer, deserializer)
    end


    # Aliases
    alias_method :textFile, :text_file
    alias_method :wholeTextFiles, :whole_text_files
    alias_method :defaultParallelism, :default_parallelism
    alias_method :setLocalProperty, :set_local_property
    alias_method :getLocalProperty, :get_local_property
    alias_method :setCallSite, :set_call_site
    alias_method :getCallSite, :get_call_site

  end
end
