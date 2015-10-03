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

    # IRB_HISTORY_FILE = File.join(Dir.home, '.irb_spark_history')
    # IRB_HISTORY_SIZE = 100

    def run
      program :name, 'RubySpark'
      program :version, Spark::VERSION
      program :description, 'Ruby wrapper for Spark'

      global_option('-d', '--debug', 'Logging message to stdout'){ $DEBUG = true }
      default_command :help


      # Build ---------------------------------------------------------------
      command :build do |c|
        c.syntax = 'build [options]'
        c.description = 'Build spark and gem extensions'
        c.option '--hadoop-version STRING', String, 'Version of hadoop which will assembled with the Spark'
        c.option '--spark-core-version STRING', String, 'Version of Spark core'
        c.option '--spark-version STRING', String, 'Version of Spark'
        c.option '--scala-version STRING', String, 'Version of Scala'
        c.option '--target STRING', String, 'Directory where Spark will be stored'
        c.option '--only-ext', 'Build only extension for RubySpark'

        c.action do |args, options|
          Spark::Build.build(options.__hash__)
          puts
          puts 'Everything is OK'
        end
      end
      alias_command :install, :build


      # Shell -----------------------------------------------------------------
      command :shell do |c|
        c.syntax = 'shell [options]'
        c.description = 'Start ruby shell for spark'
        c.option '--target STRING', String, 'Directory where Spark is stored'
        c.option '--properties-file STRING', String, 'Path to a file from which to load extra properties'
        c.option '--[no-]start', 'Start Spark immediately'
        c.option '--[no-]logger', 'Enable/disable logger (default: enable)'
        c.option '--auto-reload', 'Autoreload changed files'

        c.action do |args, options|
          options.default start: true, logger: true

          Spark.load_lib(options.target)
          Spark.logger.disable unless options.logger

          Spark.config do
            set_app_name 'RubySpark'
          end

          Spark.config.from_file(options.properties_file)

          if options.auto_reload
            require 'listen'
            listener = Listen.to(File.join(Spark.root, 'lib')) do |modified, added, removed|
              (modified+added).each do |file|
                silence_warnings { load(file) }
              end
            end
            listener.start
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


      # # IRB -------------------------------------------------------------------
      # command :irb do |c|
      #   c.syntax = 'irb [options]'
      #   c.description = 'Start ruby shell for spark'
      #   c.option '--spark-home STRING', String, 'Directory where Spark is stored'
      #   c.option '--[no-]start', 'Start Spark immediately'
      #   c.option '--[no-]logger', 'Enable/disable logger (default: enable)'
      #
      #   c.action do |args, options|
      #     options.default start: true, logger: true
      #
      #     Spark.load_lib(options.spark_home)
      #     Spark::Logger.disable unless options.logger
      #
      #     Spark.config do
      #       set_app_name 'Pry RubySpark'
      #     end
      #
      #     if options.start
      #       # Load Java and Spark
      #       Spark.start
      #       $sc = Spark.context
      #
      #       Spark.print_logo('Spark context is loaded as $sc')
      #     else
      #       Spark.print_logo('You can start Spark with Spark.start')
      #     end
      #
      #     # Load IRB
      #     require 'irb'
      #     require 'irb/completion'
      #     require 'irb/ext/save-history'
      #
      #     begin
      #       file = File.expand_path(IRB_HISTORY_FILE)
      #       if File.exists?(file)
      #         lines = IO.readlines(file).collect { |line| line.chomp }
      #         Readline::HISTORY.push(*lines)
      #       end
      #       Kernel.at_exit do
      #         lines = Readline::HISTORY.to_a.reverse.uniq.reverse
      #         lines = lines[-IRB_HISTORY_SIZE, IRB_HISTORY_SIZE] if lines.nitems > IRB_HISTORY_SIZE
      #         File.open(IRB_HISTORY_FILE, File::WRONLY | File::CREAT | File::TRUNC) { |io| io.puts lines.join("\n") }
      #       end
      #     rescue
      #     end
      #
      #     ARGV.clear # Clear Thor ARGV, otherwise IRB will parse it
      #     ARGV.concat ['--readline', '--prompt-mode', 'simple']
      #     IRB.start
      #   end
      # end


      # Home ------------------------------------------------------------------
      command :home do |c|
        c.action do |args, options|
          puts Spark.home
          exit(0)
        end
      end


      # Ruby spark jar --------------------------------------------------------
      command :ruby_spark_jar do |c|
        c.action do |args, options|
          puts Spark.ruby_spark_jar
          exit(0)
        end
      end

      run!
    end

  end
end
