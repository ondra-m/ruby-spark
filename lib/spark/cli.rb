require 'commander'

module Commander
  module UI
    # Disable paging
    # for 'classic' help
    def self.enable_paging
    end
  end
end

module Spark
  class CLI
    include Commander::Methods

    IRB_HISTORY_FILE = File.join(Dir.home, '.irb_spark_history')
    IRB_HISTORY_SIZE = 100

    def run
      program :name, 'RubySpark'
      program :version, Spark::VERSION
      program :description, 'Ruby wrapper for Spark'

      global_option('-d', '--debug', 'Logging message to stdout'){ $debug = true }
      default_command :help


      # Install ---------------------------------------------------------------
      command :install do |c|
        c.syntax = 'install [options]'
        c.description = 'Build spark and gem extensions'
        c.option '--ivy-version STRING', String, 'Version of ivy build tool'
        c.option '--hadoop-version STRING', String, 'Version of hadoop which will stored with the SPARK'
        c.option '--spark-home STRING', String, 'Directory where SPARK will be stored'
        c.option '--spark-core STRING', String, 'Version of SPARK core'
        c.option '--spark-version STRING', String, 'Version of SPARK'

        c.action do |args, options|
          options.default ivy_version: Spark::Build::DEFAULT_IVY_VERSION,
                          hadoop_version: Spark::Build::DEFAULT_HADOOP_VERSION,
                          spark_home: Spark.target_dir,
                          spark_core: Spark::Build::DEFAULT_CORE_VERSION,
                          spark_version: Spark::Build::DEFAULT_SPARK_VERSION

          Spark::Build.spark(options)
          Spark::Build.ext(options.spark_home)
          puts
          puts 'Everything is OK'
        end
      end


      # Rebuild ---------------------------------------------------------------
      command :build_ext do |c|
        c.syntax = 'build_ext [options]'
        c.description = 'Build only ruby extensions'
        c.option '--spark-home STRING', String, 'Directory where SPARK will be stored'

        c.action do |args, options|
          options.default spark_home: Spark.target_dir
          Spark::Build.ext(options.spark_home)
        end
      end
      alias_command :rebuild, :build_ext


      # Pry -------------------------------------------------------------------
      command :pry do |c|
        c.syntax = 'pry [options]'
        c.description = 'Start ruby shell for spark'
        c.option '--spark-home STRING', String, 'Directory where SPARK is stored'
        c.option '--[no-]start', 'Start SPARK immediately'
        c.option '--[no-]logger', 'Enable/disable logger (default: enable)'

        c.action do |args, options|
          options.default start: true, logger: true

          Spark.load_lib(options.spark_home)
          Spark::Logger.disable unless options.logger

          Spark.config do
            set_app_name 'Pry RubySpark'
          end

          if options.start
            # Load Java and Spark
            Spark.start
            $sc = Spark.context

            Spark.print_logo('Spark context is loaded as $sc')
          else
            Spark.print_logo('You can start Spark with Spark.start')
          end

          # Load Pry
          require 'pry'
          Pry.start
        end
      end


      # IRB -------------------------------------------------------------------
      command :irb do |c|
        c.syntax = 'irb [options]'
        c.description = 'Start ruby shell for spark'
        c.option '--spark-home STRING', String, 'Directory where SPARK is stored'
        c.option '--[no-]start', 'Start SPARK immediately'
        c.option '--[no-]logger', 'Enable/disable logger (default: enable)'

        c.action do |args, options|
          options.default start: true, logger: true

          Spark.load_lib(options.spark_home)
          Spark::Logger.disable unless options.logger

          Spark.config do
            set_app_name 'Pry RubySpark'
          end

          if options.start
            # Load Java and Spark
            Spark.start
            $sc = Spark.context

            Spark.print_logo('Spark context is loaded as $sc')
          else
            Spark.print_logo('You can start Spark with Spark.start')
          end

          # Load IRB
          require 'irb'
          require 'irb/completion'
          require 'irb/ext/save-history'

          begin
            file = File.expand_path(IRB_HISTORY_FILE)
            if File.exists?(file)
              lines = IO.readlines(file).collect { |line| line.chomp }
              Readline::HISTORY.push(*lines)
            end
            Kernel.at_exit do
              lines = Readline::HISTORY.to_a.reverse.uniq.reverse
              lines = lines[-IRB_HISTORY_SIZE, IRB_HISTORY_SIZE] if lines.nitems > IRB_HISTORY_SIZE
              File.open(IRB_HISTORY_FILE, File::WRONLY | File::CREAT | File::TRUNC) { |io| io.puts lines.join("\n") }
            end
          rescue
          end

          ARGV.clear # Clear Thor ARGV, otherwise IRB will parse it
          ARGV.concat ['--readline', '--prompt-mode', 'simple']
          IRB.start
        end
      end

      run!
    end

  end
end
