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

      @jcontext = JavaSparkContext.new(@conf)

      @conf.getAll.each do |tuple|
        @environment[EXECUTOR_ENV_KEY.size..-1] = tuple._2 if tuple._1.start_with?(EXECUTOR_ENV_KEY)
      end

    end

    def text_file(name, partitions=nil)
      partitions ||= [@jcontext.sc.defaultParallelism, 2].min
      Spark::RDD.new(@jcontext.textFile(name, partitions), self)
    end
    alias_method :textFile, :text_file

    def parallelize(array)
      Spark::RDD.new(@jcontext.parallelize(array), self)
    end

  end
end
