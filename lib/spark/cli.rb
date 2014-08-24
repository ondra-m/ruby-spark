require "thor"

module Spark
  class CLI < Thor

    IRB_HISTORY_FILE = File.join(Dir.home, ".irb_spark_history")
    IRB_HISTORY_SIZE = 100

    desc "install", "build spark and gem extensions"
    option :"ivy-version", default: Spark::Build::DEFAULT_IVY_VERSION, 
                           banner: "ivy version",
                           desc: "Version of ivy which will build the SPARK"
    option :"hadoop-version", default: Spark::Build::DEFAULT_HADOOP_VERSION, 
                              banner: "hadoop version",
                              desc: "Version of hadoop which will stored with the SPARK"
    option :"spark-home", default: Spark.target_dir,
                          banner: "directory",
                          desc: "Directory where SPARK will be stored"
    option :"spark-core", default: Spark::Build::DEFAULT_CORE_VERSION,
                          banner: "version",
                          desc: "Version of SPARK core"
    option :"spark-version", default: Spark::Build::DEFAULT_SPARK_VERSION,
                             banner: "version",
                             desc: "Version of SPARK"
    def install
      Spark::Build.spark(options)
      Spark::Build.ext(options[:"spark-home"])
    end

    desc "build_ext", "build only ruby extensions"
    option :"spark-home", default: Spark.target_dir,
                          banner: "directory of file",
                          desc: "Directory  of single jar file where SPARK is located"
    def build_ext
      Spark::Build.ext(options[:"spark-home"])
    end
    map :rebuild => :build_ext

    desc "irb", "start ruby shell for spark"
    option :spark
    def irb
      # Load Java and Spark
      Spark.load_lib(options[:spark])
      $sc = Spark::Context.new
      Spark.print_logo("Spark context is loaded as $sc")

      # Load IRB
      require "irb"
      require "irb/completion"
      require "irb/ext/save-history"

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
      ARGV.concat ["--readline", "--prompt-mode", "simple"]
      IRB.start
    end

    desc "pry", "start ruby shell for spark"
    option :spark
    def pry
      # Load Java and Spark
      Spark.load_lib(options[:spark])
      $sc = Spark::Context.new
      Spark.print_logo("Spark context is loaded as $sc")

      # Load IRB
      require "pry"
      Pry.start
    end


  end
end
