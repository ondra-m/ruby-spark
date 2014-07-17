require "tempfile"

Spark.load_lib

module Spark
  class Context

    EXECUTOR_ENV_KEY = "spark.executorEnv."

    BATCH_SIZE = 2048

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

    def text_file(name, min_partitions=nil)
      min_partitions ||= [default_parallelism, 2].min
      Spark::RDD.new(@jcontext.textFile(name, min_partitions), self, Spark::Serializer::UTF8)
    end

    # WORKAROUND to_s: PythonRDD.writeIteratorToStream works only with Array[Byte], String, Tuple2[_, _]
    # [1,2,3] is convert as [java.lang.Long, ...]
    #
    # TODO: add varible type to RubyRDD
    #       or as python => objects are written to a file and loaded through textFile
    #       or serialize to bytes
    #       auto conversion from enum to array

    # to_a -> can be Range
    def parallelize(data, num_slices=default_parallelism)
      # Spark::RDD.new(@jcontext.parallelize(array.to_a, num_slices), self)

      file = Tempfile.new("to_parallelize")

      Spark::Serializer::Simple.dump(data.to_a, file)


      file.close # not unlink

      jrdd = PythonRDD.readRDDFromFile(jcontext, file.path, num_slices)
      Spark::RDD.new(jrdd, self)
    end



    # Aliases
    alias_method :textFile, :text_file
    alias_method :defaultParallelism, :default_parallelism

  end
end
