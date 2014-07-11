require "thor"

module Spark
  class CLI < Thor

    desc "install", "build spark and gem extensions"
    option :spark
    option :target
    def install
      Spark::Build.spark(options[:target]) unless options[:spak]
      Spark::Build.ext(options[:spark])
    end

    desc "rebuild", "rebuild only ruby extensions"
    option :spark
    def rebuild
      Spark::Build.ext(options[:spark])
    end

    desc "irb", "start ruby shell for spark"
    option :spark
    def irb
      to_load = options[:spark] || Spark.target_dir

      require "irb"
      require "java"
      Dir.glob(File.join(to_load, "*")){|file| require file}
      require Spark.ruby_spark_jar

      $sc = Spark::Context.new(app_name: "RubySpark", master: "local")

      puts <<-STRING

      Welcome to
         ___       ____              __
        |   \\     / __/__  ___ _____/ /__
        | __/    _\\ \\/ _ \\/ _ `/ __/  '_/
        | \\\\    /__ / .__/\\_,_/_/ /_/\\_\\   version 1.0.0-SNAPSHOT
        |  \\\\      /_/

      Spark context is loaded as $sc

      STRING

      ARGV.clear
      IRB.start
    end
    
  end
end
