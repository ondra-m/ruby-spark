# Gems and libraries
require 'method_source'
require 'securerandom'
require 'forwardable'
require 'sourcify'
require 'socket'
require 'tempfile'
require 'tmpdir'
require 'json'

module Spark
  autoload :Context,        'spark/context'
  autoload :Config,         'spark/config'
  autoload :RDD,            'spark/rdd'
  autoload :CLI,            'spark/cli'
  autoload :Build,          'spark/build'
  autoload :Serializer,     'spark/serializer'
  autoload :Helper,         'spark/helper'
  autoload :StorageLevel,   'spark/storage_level'
  autoload :Command,        'spark/command'
  autoload :CommandBuilder, 'spark/command_builder'
  autoload :Sampler,        'spark/sampler'
  autoload :Logger,         'spark/logger'
  autoload :JavaBridge,     'spark/java_bridge'
  autoload :ExternalSorter, 'spark/sort'
  autoload :Constant,       'spark/constant'
  autoload :Broadcast,      'spark/broadcast'
  autoload :Accumulator,    'spark/accumulator'
  autoload :StatCounter,    'spark/stat_counter'
  autoload :Library,        'spark/library'

  # Mllib
  autoload :Mllib, 'spark/mllib'

  # SQL
  autoload :SQL,        'spark/sql'
  autoload :SQLContext, 'spark/sql'

  include Helper::System

  DEFAULT_CONFIG_FILE = File.join(Dir.home, '.ruby-spark.conf')

  def self.print_logo(message=nil)
    puts <<-STRING

    Welcome to
                  __           ____              __
        ______ __/ /  __ __   / __/__  ___ _____/ /__
       / __/ // / _ \\/ // /  _\\ \\/ _ \\/ _ `/ __/  '_/
      /_/  \\_,_/_.__/\\_, /  /___/ .__/\\_,_/_/ /_/\\_\\   version #{Spark::VERSION}
                    /___/      /_/

    #{message}

    STRING
  end

  # Returns current configuration. Configurations can be changed until
  # context is initialized. In this case config is locked only for reading.
  #
  # == Configuration can be changed:
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
  def self.context
    @context
  end

  # Current active SQLContext or nil.
  def self.sql_context
    @sql_context
  end

  # Initialize spark context if not already. Config will be automatically
  # loaded on constructor. From that point `config` will use configuration
  # from running Spark and will be locked only for reading.
  def self.start
    @context ||= Spark::Context.new
  end

  def self.start_sql
    @sql_context ||= Spark::SQL::Context.new(start)
  end

  def self.stop
    @context.stop
    RubyWorker.stopServer
    logger.info('Workers were stopped')
  rescue
    nil
  ensure
    @context = nil
    @sql_context = nil
    clear_config
  end

  def self.started?
    !!@context
  end


  # ===============================================================================
  # Defaults

  # Load default configuration for Spark and RubySpark
  # By default are values stored at ~/.ruby-spark.conf
  # File is automatically created
  def self.load_defaults
    unless File.exists?(DEFAULT_CONFIG_FILE)
      save_defaults_to(DEFAULT_CONFIG_FILE)
    end

    load_defaults_from(DEFAULT_CONFIG_FILE)
  end

  # Clear prev setting and load new from file
  def self.load_defaults_from(file_path)
    # Parse values
    values = File.readlines(file_path)
    values.map!(&:strip)
    values.select!{|value| value.start_with?('gem.')}
    values.map!{|value| value.split(nil, 2)}
    values = Hash[values]

    # Clear prev values
    @target_dir = nil
    @ruby_spark_jar = nil
    @spark_home = nil

    # Load new
    @target_dir = values['gem.target']
  end

  # Create target dir and new config file
  def self.save_defaults_to(file_path)
    dir = File.join(Dir.home, ".ruby-spark.#{SecureRandom.uuid}")

    if Dir.exist?(dir)
      save_defaults_to(file_path)
    else
      Dir.mkdir(dir, 0700)
      file = File.open(file_path, 'w')
      file.puts "# Directory where will be Spark saved"
      file.puts "gem.target   #{dir}"
      file.puts ""
      file.puts "# You can also defined spark properties"
      file.puts "# spark.master                       spark://master:7077"
      file.puts "# spark.ruby.serializer              marshal"
      file.puts "# spark.ruby.serializer.batch_size   2048"
      file.close
    end
  end


  # ===============================================================================
  # Global settings and variables

  def self.logger
    @logger ||= Spark::Logger.new
  end

  # Root of the gem
  def self.root
    @root ||= File.expand_path('..', File.dirname(__FILE__))
  end

  # Default directory for java extensions
  def self.target_dir
    @target_dir ||= File.join(root, 'target')
  end

  # Directory where is worker.rb
  def self.worker_dir
    @worker_dir ||= File.join(root, 'lib', 'spark', 'worker')
  end

  def self.ruby_spark_jar
    @ruby_spark_jar ||= File.join(target_dir, 'ruby-spark.jar')
  end

  def self.spark_ext_dir
    @spark_ext_dir ||= File.join(root, 'ext', 'spark')
  end


  # ===============================================================================
  # Load JVM and jars

  # Load dependent libraries, can be use once
  # Cannot load before CLI::install
  #
  # == Parameters:
  # target::
  #   path to directory where are located sparks .jar files or single Spark jar
  #
  def self.load_lib(target=nil)
    return if @java_bridge

    target ||= Spark.target_dir

    @java_bridge = JavaBridge.init(target)
    @java_bridge.import_all
    nil
  end

  def self.java_bridge
    @java_bridge
  end


  # Aliases
  class << self
    alias_method :sc, :context
    alias_method :jb, :java_bridge
    alias_method :home, :root
  end

end

# C/Java extensions
require 'ruby_spark_ext'

# Ruby core extensions
require 'spark/ext/module'
require 'spark/ext/object'
require 'spark/ext/hash'
require 'spark/ext/string'
require 'spark/ext/integer'
require 'spark/ext/ip_socket'
require 'spark/ext/io'

# Other requirments
require 'spark/version'
require 'spark/error'

# Load default settings for gem and Spark
Spark.load_defaults

# Make sure that Spark be always stopped
Kernel.at_exit do
  begin
    Spark.started? && Spark.stop
  rescue
  end
end
