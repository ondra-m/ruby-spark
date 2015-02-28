module Spark
  module Build

    DEFAULT_SCALA_VERSION  = '2.10.4'
    DEFAULT_CORE_VERSION   = '2.10'
    DEFAULT_SPARK_VERSION  = '1.2.1'
    DEFAULT_HADOOP_VERSION = '2.4.0'

    SBT_FULL     = 'sbt/sbt assemblyPackageDependency package clean'
    SBT_ONLY_EXT = 'sbt/sbt package clean'

    def self.build(options)
      spark_home     = options.spark_home     || Spark.target_dir
      scala_version  = options.scala_version  || DEFAULT_SCALA_VERSION
      spark_core     = options.spark_core     || DEFAULT_CORE_VERSION
      spark_version  = options.spark_version  || DEFAULT_SPARK_VERSION
      hadoop_version = options.hadoop_version || DEFAULT_HADOOP_VERSION
      only_ext       = options.only_ext

      env = {
        "SCALA_VERSION" => scala_version,
        "SPARK_VERSION" => spark_version,
        "SPARK_CORE_VERSION" => spark_core,
        "HADOOP_VERSION" => hadoop_version,
        "SPARK_HOME" => spark_home
      }

      if only_ext
        cmd = SBT_ONLY_EXT
      else
        cmd = SBT_FULL
      end

      Dir.chdir(Spark.spark_ext_dir) do
        unless Kernel.system(env, cmd)
          raise Spark::BuildError, 'Spark cannot be assembled.'
        end
      end
    end

    private

      # def self.compile_ext(classpath)
      #   ['scalac',
      #    '-d', File.join(Spark.target_dir, Spark.ruby_spark_jar),
      #    '-classpath', "\"#{File.join(classpath, '*')}\"",
      #    File.join(Spark.spark_ext_dir, 'src', 'main', 'scala', '*.scala')].join(' ')
      # end

  end
end
