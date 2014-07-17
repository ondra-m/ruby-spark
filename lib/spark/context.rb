Spark.load_lib

module Spark
  class Context

    EXECUTOR_ENV_KEY = "spark.executorEnv."

    attr_reader :conf, :environment, :jcontext, :java_accumulator

    def initialize(options={})
      @options = options

      @environment = {}

      @conf = SparkConf.new(true)
      @conf.setAppName(options[:app_name])
      @conf.setMaster(options[:master])
      @conf.set("spark.default.parallelism", "2")

      @jcontext = JavaSparkContext.new(@conf)

      @conf.getAll.each do |tuple|
        @environment[EXECUTOR_ENV_KEY.size..-1] = tuple._2 if tuple._1.start_with?(EXECUTOR_ENV_KEY)
      end

    end

    def default_parallelism
      @jcontext.sc.defaultParallelism
    end

    def text_file(name, partitions=nil)
      partitions ||= [default_parallelism, 2].min
      Spark::RDD.new(@jcontext.textFile(name, partitions), self)
    end

    # WORKAROUND to_s: PythonRDD.writeIteratorToStream works only with Array[Byte], String, Tuple2[_, _]
    # [1,2,3] is convert as [java.lang.Long, ...]
    #
    # TODO: add varible type to RubyRDD
    #       or as python => objects are written to a file and loaded through textFile
    #       or serialize to bytes
    #       auto conversion from enum to array
    def parallelize(array, num_slices=default_parallelism)
      Spark::RDD.new(@jcontext.parallelize(array.map!(&:to_s), num_slices), self)
    end



    # Aliases
    alias_method :textFile, :text_file
    alias_method :defaultParallelism, :default_parallelism

  end
end
