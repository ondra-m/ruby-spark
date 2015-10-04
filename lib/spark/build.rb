module Spark
  module Build

    DEFAULT_SCALA_VERSION  = '2.10.4'
    DEFAULT_CORE_VERSION   = '2.10'
    DEFAULT_SPARK_VERSION  = '1.5.1'
    DEFAULT_HADOOP_VERSION = '1.0.4'

    SBT       = 'sbt/sbt'
    SBT_DEPS  = 'assemblyPackageDependency'
    SBT_EXT   = 'package'
    SBT_CLEAN = 'clean'

    def self.build(options={})
      scala_version      = options[:scala_version]      || DEFAULT_SCALA_VERSION
      spark_core_version = options[:spark_core_version] || DEFAULT_CORE_VERSION
      spark_version      = options[:spark_version]      || DEFAULT_SPARK_VERSION
      hadoop_version     = options[:hadoop_version]     || DEFAULT_HADOOP_VERSION
      target             = options[:target]             || Spark.target_dir
      only_ext           = options[:only_ext]           || false

      env = {
        'SCALA_VERSION' => scala_version,
        'SPARK_VERSION' => spark_version,
        'SPARK_CORE_VERSION' => spark_core_version,
        'HADOOP_VERSION' => hadoop_version,
        'TARGET_DIR' => target
      }

      cmd = [SBT]
      cmd << SBT_EXT
      cmd << SBT_DEPS unless only_ext
      cmd << SBT_CLEAN unless $DEBUG

      Dir.chdir(Spark.spark_ext_dir) do
        unless Kernel.system(env, cmd.join(' '))
          raise Spark::BuildError, 'Spark cannot be assembled.'
        end
      end
    end

  end
end
