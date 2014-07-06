module Spark
  class Context

    EXECUTOR_ENV_KEY = "spark.executorEnv."

    attr_reader :conf, :environment, :context, :java_accumulator

    def initialize(options={})
      @options = options

      @environment = {}

      @conf = SparkConf.new(true)
      @conf.setAppName(options[:app_name])
      @conf.setMaster(options[:master])

      @context = JavaSparkContext.new(@conf)

      @conf.getAll.each do |tuple|
        @environment[EXECUTOR_ENV_KEY.size..-1] = tuple._2 if tuple._1.start_with?(EXECUTOR_ENV_KEY)
      end

    end

    def text_file(name, partitions=nil)
      partitions ||= [@context.sc.defaultParallelism, 2].min
      Spark::RDD.new(@context.textFile(name, partitions), self)
    end

  end
end
