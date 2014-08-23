# Necessary libraries
Spark.load_lib

# Common configuration for RubySpark and Spark
#
module Spark
  class Config

    include Spark::Helper::Platform

    attr_reader :spark_conf

    def self.show(spark_conf)
      Hash[spark_conf.getAll.map{|tuple| [tuple._1, tuple._2]}]
    end

    def initialize
      # true => load default configuration
      @spark_conf = SparkConf.new(true)
      set_default
    end

    def parse(options)
      return self if !options.is_a?(Hash)
      options.stringify_keys!
      
      options["spark.app.name"] = options.delete("app_name") if options.has_key?("app_name")
      options["spark.master"]   = options.delete("master")   if options.has_key?("master")

      options.each {|key, value| set(key, value)}
      self
    end

    def valid!
      if !@spark_conf.contains("spark.app.name")
        raise Spark::ConfigurationError, "An application name must be set in your configuration"
      end

      if !@spark_conf.contains("spark.master")
        raise Spark::ConfigurationError, "A master URL must be set in your configuration"
      end

      if Spark::Serializer.get(@spark_conf.get("spark.ruby.serializer")).nil?
        raise Spark::ConfigurationError, "Default serializer must be set in your configuration"
      end
    end

    def show
      Config.show(@spark_conf)
    end

    def set(key, value)
      @spark_conf.set(key, value)
    end

    def set_app_name(name)
      @spark_conf.setAppName(default_app_name)
    end

    def set_master(master)
      @spark_conf.setMaster(default_master)
    end

    # =============================================================================
    # Defaults

    def set_default
      set_app_name(default_app_name)
      set_master(default_master)
      set("spark.ruby.worker_type", default_worker_type)
      set("spark.ruby.parallelize_strategy", default_parallelize_strategy)
      set("spark.ruby.serializer", default_serializer)
      set("spark.ruby.batch_size", default_batch_size)
    end

    def default_app_name
      "RubySpark"
    end

    def default_master
      "local[*]"
    end

    def default_serializer
      ENV["SPARK_RUBY_SERIALIZER"] || Spark::Serializer::DEFAULT_SERIALIZER_NAME
    end

    def default_batch_size
      ENV["SPARK_RUBY_BATCH_SIZE"] || Spark::Serializer::DEFAULT_BATCH_SIZE.to_s
    end

    # Default level of worker type. Fork doesn't work on jruby and windows.
    #
    #   Thread: all workers are created via thread
    #   Process: workers are created by fork
    #   Simple: workers are created by Spark as single process
    #
    def default_worker_type
      ENV["SPARK_RUBY_WORKER_TYPE"] || begin
        if jruby? || windows?
          "thread"
        else
          "process"
        end
      end
    end

    # How to handle with data in method parallelize.
    #
    #   inplace: data are changed directly to save memory
    #   deep_copy: data are cloned fist
    #
    def default_parallelize_strategy
      ENV["SPARK_RUBY_PARALLELIZE_STRATEGY"] || "inplace"
    end



    # Aliases
    alias_method :setAppName, :set_app_name
    alias_method :setMaster,  :set_master

  end
end
