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

    # use: memory => direct serialization
    #      file => through file
    def parallelize(data, num_slices=nil, use=:memory)
      num_slices ||= default_parallelism
      data = data.to_a

      case use
      when :memory
        Spark::Serializer::Simple.dump_to_java(data)
        jrdd = jcontext.parallelize(data, num_slices)
      when :file
        file = Tempfile.new("to_parallelize")
        Spark::Serializer::Simple.dump(data, file)
        file.close # not unlink
        
        jrdd = PythonRDD.readRDDFromFile(jcontext, file.path, num_slices)
      end

      Spark::RDD.new(jrdd, self, Spark::Serializer::Simple)
    end



    # Aliases
    alias_method :textFile, :text_file
    alias_method :defaultParallelism, :default_parallelism

  end
end
