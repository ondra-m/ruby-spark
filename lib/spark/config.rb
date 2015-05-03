# Necessary libraries
Spark.load_lib

module Spark
  # Common configuration for RubySpark and Spark
  class Config

    include Spark::Helper::System

    TYPES = {
      'spark.shuffle.spill' => :boolean,
      'spark.ruby.batch_size' => :integer
    }

    # Initialize java SparkConf and load default configuration.
    def initialize
      @spark_conf = SparkConf.new(true)
      set_default
    end

    def from_file(file)
      check_read_only

      if file && File.exist?(file)
        file = File.expand_path(file)
        RubyUtils.loadPropertiesFile(spark_conf, file)
      end
    end

    def [](key)
      get(key)
    end

    def []=(key, value)
      set(key, value)
    end

    def spark_conf
      if Spark.started?
        # Get latest configuration
        Spark.context.jcontext.conf
      else
        @spark_conf
      end
    end

    def valid!
      errors = []

      if !contains?('spark.app.name')
        errors << 'An application name must be set in your configuration.'
      end

      if !contains?('spark.master')
        errors << 'A master URL must be set in your configuration.'
      end

      if Spark::Serializer.get(get('spark.ruby.serializer')).nil?
        errors << 'Default serializer must be set in your configuration.'
      end

      scanned = get('spark.ruby.executor.command').scan('%s')

      if scanned.size == 0
        errors << "Executor command must contain '%s'."
      end

      if scanned.size > 1
        errors << "Executor command can contain only one '%s'."
      end

      if errors.any?
        errors.map!{|error| "- #{error}"}

        raise Spark::ConfigurationError, "Configuration is not valid:\r\n#{errors.join("\r\n")}"
      end
    end

    def read_only?
      Spark.started?
    end

    # Rescue from NoSuchElementException
    def get(key)
      value = spark_conf.get(key.to_s)

      case TYPES[key]
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

    def contains?(key)
      spark_conf.contains(key.to_s)
    end

    def set(key, value)
      check_read_only
      spark_conf.set(key.to_s, value.to_s)
    end

    def set_app_name(name)
      set('spark.app.name', name)
    end

    def set_master(master)
      set('spark.master', master)
    end

    def parse_boolean(value)
      case value
      when 'true'
        true
      when 'false'
        false
      end
    end

    def parse_integer(value)
      value.to_i
    end

    # =============================================================================
    # Defaults

    def set_default
      set_app_name('RubySpark')
      set_master('local[*]')
      set('spark.ruby.driver_home', Spark.home)
      set('spark.ruby.parallelize_strategy', default_parallelize_strategy)
      set('spark.ruby.serializer', default_serializer)
      set('spark.ruby.batch_size', default_batch_size)
      set('spark.ruby.executor.uri', default_executor_uri)
      set('spark.ruby.executor.command', default_executor_command)
      set('spark.ruby.executor.options', default_executor_options)
      set('spark.ruby.worker.type', default_worker_type)
      load_worker_envs
    end

    # How to handle with data in method parallelize.
    #
    # == Possible options:
    # inplace:: data are changed directly to save memory
    # deep_copy:: data are cloned fist
    #
    def default_parallelize_strategy
      ENV['SPARK_RUBY_PARALLELIZE_STRATEGY'] || 'inplace'
    end

    def default_serializer
      ENV['SPARK_RUBY_SERIALIZER'] || Spark::Serializer::DEFAULT_SERIALIZER_NAME
    end

    def default_batch_size
      ENV['SPARK_RUBY_BATCH_SIZE'] || Spark::Serializer::DEFAULT_BATCH_SIZE.to_s
    end

    # Ruby executor.
    #
    # == Options:
    # nil::
    #   System's gem is loaded (ruby-spark).
    #
    # other::
    #   Path of library which will be used.
    #   Current ruby-spark gem is used.
    #   (default)
    #
    def default_executor_uri
      ENV['SPARK_RUBY_EXECUTOR_URI'] || ''
    end

    # Command template which is applied when scala want create a ruby
    # process (e.g. master, home request). Command is represented by '%s'.
    #
    # == Example:
    #   bash --norc -i -c "export HOME=/home/user; cd; source .bashrc; %s"
    #
    def default_executor_command
      ENV['SPARK_RUBY_EXECUTOR_COMMAND'] || '%s'
    end

    # Options for every worker.
    #
    # == Examples:
    #   -J-Xmx512m
    #
    def default_executor_options
      ENV['SPARK_RUBY_EXECUTOR_OPTIONS'] || ''
    end

    # Type of worker.
    #
    # == Options:
    # process:: (default)
    # thread:: (experimental)
    #
    def default_worker_type
      ENV['SPARK_RUBY_WORKER_TYPE'] || 'process'
    end

    # Load environment variables for worker from ENV.
    #
    # == Examples:
    #   SPARK_RUBY_WORKER_ENV_KEY1="1"
    #   SPARK_RUBY_WORKER_ENV_KEY2="2"
    #
    def load_worker_envs
      prefix = 'SPARK_RUBY_WORKER_ENV_'

      envs = ENV.select{|key, _| key.start_with?(prefix)}
      envs.each do |key, value|
        key = key.dup # ENV keys are frozen
        key.slice!(0, prefix.size)

        set("spark.ruby.worker.env.#{key}", value)
      end
    end

    # Aliases
    alias_method :getAll,     :get_all
    alias_method :setAppName, :set_app_name
    alias_method :setMaster,  :set_master

    private

      def check_read_only
        if read_only?
          raise Spark::ConfigurationError, 'Configuration is ready only'
        end
      end

  end
end
