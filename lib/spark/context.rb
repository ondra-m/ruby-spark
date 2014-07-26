require "tempfile"

# Necessary libraries
Spark.load_lib

# Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
# cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
#
module Spark
  class Context

    EXECUTOR_ENV_KEY = "spark.executorEnv."

    BATCH_SIZE = 2048

    attr_reader :environment, :jcontext

    # Constructor fo Ruby context
    # Required parameters: app_name and master
    #
    def initialize(options={})
      @environment = {}

      # true - load default configuration
      @conf = SparkConf.new(true)
      @conf.setAppName(options[:app_name])
      @conf.setMaster(options[:master])

      raise Spark::ConfigurationError, "A master URL must be set in your configuration" unless @conf.contains("spark.master")
      raise Spark::ConfigurationError, "An application name must be set in your configuration" unless @conf.contains("spark.app.name")

      @jcontext = JavaSparkContext.new(@conf)

      set_call_site("Ruby") # description of stage

      @conf.getAll.each do |tuple|
        @environment[EXECUTOR_ENV_KEY.size..-1] = tuple._2 if tuple._1.start_with?(EXECUTOR_ENV_KEY)
      end
    end

    # Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD)
    def default_parallelism
      @jcontext.sc.defaultParallelism
    end

    # Set a local property that affects jobs submitted from this thread, such as the
    # Spark fair scheduler pool.
    def set_local_property(key, value)
      jcontext.setLocalProperty(key, value)
    end

    # Get a local property set in this thread, or null if it is missing
    def get_local_property(key)
      jcontext.getLocalProperty(key)
    end

    # Support function for API backtraces.
    def set_call_site(site)
      set_local_property("externalCallSite", site)
    end

    # Capture the current user callsite and return a formatted version for printing. If the user
    # has overridden the call site, this will return the user's version.
    def get_call_site
      jcontext.getCallSite
    end

    # Read a text file from HDFS, a local file system (available on all nodes), or any
    # Hadoop-supported file system URI, and return it as an RDD of Strings.
    #
    def text_file(path, min_partitions=nil)
      min_partitions ||= default_parallelism
      Spark::RDD.new(@jcontext.textFile(path, min_partitions), self, Spark::Serializer::UTF8)
    end

    # Distribute a local Ruby collection to form an RDD
    # Direct method can be slow so be careful, this method update data inplace
    #
    #   data: Range or Array
    #   num_slices: number of slice
    #   user: direct => direct serialization
    #         file => through file
    #
    def parallelize(data, num_slices=nil, use=:direct)
      num_slices ||= default_parallelism
      data = data.to_a # for enumerator

      case use
      when :direct
        Spark::Serializer::Simple.dump_to_java(data)
        jrdd = jcontext.parallelize(data, num_slices)
      when :file
        file = Tempfile.new("to_parallelize")
        Spark::Serializer::Simple.dump(data, file)
        file.close # not unlink
        jrdd = RubyRDD.readRDDFromFile(jcontext, file.path, num_slices)
        file.unlink
      end

      Spark::RDD.new(jrdd, self, Spark::Serializer::Simple)
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
