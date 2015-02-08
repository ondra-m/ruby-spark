# Necessary libraries
Spark.load_lib

# Common configuration for RubySpark and Spark
#
module Spark
  class Config

    include Spark::Helper::System

    PROPERTIES = {
      "spark.shuffle.spill" => :boolean,
      "spark.ruby.batch_size" => :integer,
      "spark.ruby.accumulator_connection" => :integer
    }

    # Initialize java SparkConf and load default configuration.
    def initialize
      @spark_conf = SparkConf.new(true)
      set_default
    end

    def [](key)
      get(key)
    end

    def []=(key, value)
      set(key, value)
    end

    def spark_conf
      if Spark.started?
        Spark.context.jcontext.conf
      else
        @spark_conf
      end
    end

    def valid!
      if !contains("spark.app.name")
        raise Spark::ConfigurationError, "An application name must be set in your configuration"
      end

      if !contains("spark.master")
        raise Spark::ConfigurationError, "A master URL must be set in your configuration"
      end

      if Spark::Serializer.get(get("spark.ruby.serializer")).nil?
        raise Spark::ConfigurationError, "Default serializer must be set in your configuration"
      end
    end

    def read_only?
      Spark.started?
    end

    # Rescue from NoSuchElementException
    def get(key)
      value = spark_conf.get(key.to_s)

      case PROPERTIES[key]
      when :boolean
        parse_boolean(value)
      when :integer
        parse_integer(value)
      else
        value
      end
    rescue
      nil
    end

    def get_all
      Hash[spark_conf.getAll.map{|tuple| [tuple._1, tuple._2]}]
    end

    def contains(key)
      spark_conf.contains(key.to_s)
    end

    def set(key, value)
      if read_only?
        raise Spark::ConfigurationError, "Configuration is ready only"
      else
        spark_conf.set(key.to_s, value.to_s)
      end
    end

    def set_app_name(name)
      set("spark.app.name", name)
    end

    def set_master(master)
      set("spark.master", master)
    end

    def parse_boolean(value)
      case value
      when "true"
        true
      when "false"
        false
      end
    end

    def parse_integer(value)
      value.to_i
    end

    # =============================================================================
    # Defaults

    def set_default
      set_app_name(default_app_name)
      set_master(default_master)
      set("spark.ruby.worker.type", default_worker_type)
      set("spark.ruby.worker.arguments", default_worker_arguments)
      set("spark.ruby.worker.memory", default_worker_memory)
      set("spark.ruby.parallelize_strategy", default_parallelize_strategy)
      set("spark.ruby.serializer", default_serializer)
      set("spark.ruby.batch_size", default_batch_size)
      set("spark.ruby.accumulator_connection", default_accumulator_connection)
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

    def default_worker_type
      ENV["SPARK_RUBY_WORKER_TYPE"] || "process"
    end

    def default_worker_arguments
      ENV["SPARK_RUBY_WORKER_ARGUMENTS"] || ""
    end

    def default_worker_memory
      ENV["SPARK_RUBY_WORKER_MEMORY"] || ""
    end

    def default_accumulator_connection
      ENV["SPARK_RUBY_ACCUMULATOR_CONNECTION"] || 2
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
    alias_method :getAll,     :get_all
    alias_method :setAppName, :set_app_name
    alias_method :setMaster,  :set_master

  end
end
