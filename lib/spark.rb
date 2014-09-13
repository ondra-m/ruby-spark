# TODO: - kill worker without controll socket to master
#       e.g. controll thread for every worker
#       - přidat operaci undo pro Command, přepínač kde jze zvolit
#       že nebude inplace edit - když bude chyba tak se vrátí
#       poslední správný výsledek
#       - do spaen dávat rlimits

require "spark/ext/array"
require "spark/ext/hash"
require "spark/ext/string"
require "spark/version"
require "spark/error"

module Spark
  autoload :Context,        "spark/context"
  autoload :Config,         "spark/config"
  autoload :RDD,            "spark/rdd"
  autoload :CLI,            "spark/cli"
  autoload :Build,          "spark/build"
  autoload :Serializer,     "spark/serializer"
  autoload :Helper,         "spark/helper"
  autoload :StorageLevel,   "spark/storage_level"
  autoload :Command,        "spark/worker/command"
  autoload :CommandBuilder, "spark/command_builder"
  autoload :Sampler,        "spark/sampler"

  extend Helper::Platform

  def self.print_logo(message=nil)
    puts <<-STRING

    Welcome to
       ___       ____              __
      | _ \\     / __/__  ___ _____/ /__
      | __/    _\\ \\/ _ \\/ _ `/ __/  '_/
      | \\\\    /__ / .__/\\_,_/_/ /_/\\_\\   version #{Spark::VERSION}
      |  \\\\      /_/

    #{message}

    STRING
  end

  # Returns current configuration. Configurations can be changed until
  # context is initialized. In this case config is locked only for reading.
  #
  # Configuration can be changed:
  #
  #   Spark.config.set('spark.app.name', 'RubySpark')
  #
  #   Spark.config['spark.app.name'] = 'RubySpark'
  #
  #   Spark.config do
  #     set 'spark.app.name', 'RubySpark'
  #   end
  #
  def self.config(&block)
    @config ||= Spark::Config.new

    if block_given?
      @config.instance_eval(&block)
    else
      @config
    end
  end

  # Destroy current configuration. This can be useful for restarting config
  # to set new. It has no effect if context is already started.
  def self.clear_config
    @config = nil
  end

  # Return a current active context or nil.
  #
  # TODO: Run `start` if context is nil?
  #
  def self.context
    @context
  end

  # Initialize spark context if not already. Config will be automatically 
  # loaded on constructor. From that point `config` will use configuration
  # from running Spark and will be locked only for reading.
  def self.start
    if started?
      # Already started
    else
      @context ||= Spark::Context.new
    end
  end

  def self.stop
    @context.stop
    RubyWorker.stopServer
  rescue
    nil
  ensure
    @context = nil
    clear_config
  end

  def self.started?
    # !!(@config && @context)
    !!@context
  end

  # Root of the gem
  def self.root
    @root ||= File.expand_path("..", File.dirname(__FILE__))
  end

  # Default directory for java extensions
  def self.target_dir
    @target_dir ||= File.join(root, 'target')
  end

  # Directory where is worker.rb
  def self.worker_dir
    @worker_dir ||= File.join(root, 'lib', 'spark', 'worker')
  end

  # Full path of ruby spark extension
  # used for build and load
  def self.ruby_spark_jar
    @ruby_spark_jar ||= File.join(target_dir, 'ruby-spark.jar')
  end

  def self.ivy_xml
    @ivy_xml ||= File.join(root, 'ivy', 'ivy.xml')
  end

  # ===============================================================================
  # Global Spark actions

  # Disable all Spark log
  def self.disable_log
    load_lib
    JLogger.getLogger("org").setLevel(JLevel.toLevel("OFF"))
    JLogger.getLogger("akka").setLevel(JLevel.toLevel("OFF"))
    JLogger.getRootLogger().setLevel(JLevel.toLevel("OFF"))
  end

  # ===============================================================================
  # Load JVM and jars

  JAVA_OBJECTS = [
    "org.apache.spark.SparkConf",
    "org.apache.spark.api.java.JavaSparkContext",
    "org.apache.spark.api.ruby.RubyRDD",
    "org.apache.spark.api.ruby.RubyWorker",
    "org.apache.spark.api.ruby.PairwiseRDD",
    "org.apache.spark.api.python.PythonPartitioner",
    :JLogger => "org.apache.log4j.Logger",
    :JLevel  => "org.apache.log4j.Level",
    :JStorageLevel => "org.apache.spark.storage.StorageLevel"
  ]

  # Load dependent libraries, can be use once
  # Cannot load before CLI::install
  #
  #   spark_home: path to directory where are located sparks .jar files
  #               or single Spark jar
  #
  def self.load_lib(spark_home=nil)
    return if @loaded_lib

    spark_home ||= Spark.target_dir

    if jruby?
      jruby_load_lib(spark_home)
    else
      rjb_load_lib(spark_home)
    end

    @loaded_lib = true
  end

  def self.jruby_load_lib(spark_home)
    require "java"

    get_jars(spark_home).each {|jar| require jar}

    java_objects.each do |key, value|
      Object.const_set(key, eval(value))
    end
  end

  def self.rjb_load_lib(spark_home)
    raise Spark::ConfigurationError, "Environment variable JAVA_HOME is not set" unless ENV.has_key?("JAVA_HOME")
    require "rjb"

    separator = windows? ? ';' : ':'

    jars = get_jars(spark_home).join(separator)
    Rjb::load(jars)
    Rjb::primitive_conversion = true

    java_objects.each do |key, value|
      # Avoid 'already initialized constant'
      Object.const_set(key, silence_warnings { Rjb::import(value) })
    end
  end

  def self.get_jars(spark_home)
    jars = []
    if File.file?(spark_home)
      jars << spark_home
    else
      jars << Dir.glob(File.join(spark_home, "*.jar"))
    end
    jars << Spark.ruby_spark_jar
    jars.flatten
  end

  def self.java_objects
    hash = {}
    JAVA_OBJECTS.each do |object|
      if object.is_a?(Hash)
        hash.merge!(object)
      else
        key = object.split(".").last.to_sym
        hash[key] = object
      end
    end
    hash
  end


  # ===============================================================================
  # Others

  def self.silence_warnings
    old_verbose, $VERBOSE = $VERBOSE, nil
    yield
  ensure
    $VERBOSE = old_verbose
  end

end

Kernel::at_exit do
  begin
    Spark.stop
  rescue
  end
end
