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

    # Constructor fo Ruby context
    # Required parameters: app_name and master
    #
    def initialize(arg=nil)
      if arg.is_a?(Spark::Config)
        config = arg
      else
        config = Spark::Config.new
        config.parse(arg)
      end
      config.valid!

      @jcontext = JavaSparkContext.new(config.spark_conf)

      set_call_site("Ruby") # description of stage
    end

    # Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD)
    #
    def default_parallelism
      @jcontext.sc.defaultParallelism
    end

    def get_serializer(serializer, batch_size=nil)
      serializer   = Spark::Serializer.get(serializer)
      serializer ||= Spark::Serializer.get(config("spark.ruby.serializer"))
      serializer.new(batch_size || config("spark.ruby.batch_size"))
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
        config[key]
      else
        @config ||= Spark::Config.show(jcontext.conf)
      end
    end

    # Read a text file from HDFS, a local file system (available on all nodes), or any
    # Hadoop-supported file system URI, and return it as an RDD of Strings.
    #
    def text_file(path, min_partitions=nil, options={})
      min_partitions ||= default_parallelism
      serializer = get_serializer(options[:serializer], options[:batch_size])

      Spark::RDD.new(@jcontext.textFile(path, min_partitions), self, serializer, get_serializer("UTF8"))
    end

    # Distribute a local Ruby collection to form an RDD
    # Direct method can be slow so be careful, this method update data inplace
    #
    #   data: Range or Array
    #   num_slices: number of slice
    #   user: direct => direct serialization
    #         file => through file
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


    # Aliases
    alias_method :textFile, :text_file
    alias_method :defaultParallelism, :default_parallelism
    alias_method :setLocalProperty, :set_local_property
    alias_method :getLocalProperty, :get_local_property
    alias_method :setCallSite, :set_call_site
    alias_method :getCallSite, :get_call_site

  end
end
