require 'open3'

module Spark
  module Build

    DEFAULT_IVY_VERSION    = '2.3.0'
    DEFAULT_CORE_VERSION   = '2.10'
    DEFAULT_SPARK_VERSION  = '1.2.0'
    DEFAULT_HADOOP_VERSION = '2.4.0'

    def self.spark(options={})
      ivy_version    = options.ivy_version    || DEFAULT_IVY_VERSION
      spark_home     = options.spark_home     || Spark.target_dir
      spark_core     = options.spark_core     || DEFAULT_CORE_VERSION
      spark_version  = options.spark_version  || DEFAULT_SPARK_VERSION
      hadoop_version = options.hadoop_version || DEFAULT_HADOOP_VERSION

      dir = Dir.mktmpdir
      ivy_jar = File.join(dir, 'ivy.jar')

      begin
        print 'Building ivy'
        exec(get_ivy(ivy_jar, ivy_version))
        print 'Building spark'
        exec(get_spark(dir, ivy_jar, spark_core, spark_version, hadoop_version))
        print 'Moving files'
        FileUtils.mkdir_p(spark_home)
        FileUtils.mv(Dir.glob(File.join(dir, 'spark', '*')), spark_home)
        puts ' ... OK'
      rescue Exception => e
        raise Spark::BuildError, "Cannot build Spark. #{e}"
      ensure
        FileUtils.remove_entry(dir)
      end
    end

    def self.ext(spark_home=nil)
      spark_home ||= Spark.target_dir

      begin
        print 'Building ruby-spark extension'
        exec(compile_ext(spark_home))
      rescue Exception => e
        raise Spark::BuildError, "Cannot build ruby-spark extension. #{e}", e.backtrace
      end
    end

    private

      def self.get_ivy(ivy_jar, version)
        ['curl',
         '-L',
         '-o', ivy_jar,
         "http://search.maven.org/remotecontent\?filepath\=org/apache/ivy/ivy/#{version}/ivy-#{version}.jar"].join(' ')
      end

      def self.get_spark(dir, ivy_jar, core_version, spark_version, hadoop_version)
        ['java',
         "-Dspark.core.version=#{core_version}",
         "-Dspark.version=#{spark_version}",
         "-Dhadoop.version=#{hadoop_version}",
         '-jar', ivy_jar,
         '-ivy', Spark.ivy_xml,
         '-retrieve', "\"#{File.join(dir, 'spark', '[artifact]-[revision](-[classifier]).[ext]')}\""].join(' ')
      end

      def self.compile_ext(classpath)
        ['scalac',
         '-d', Spark.ruby_spark_jar,
         '-classpath', "\"#{File.join(classpath, '*')}\"",
         File.join(Spark.root, 'src', 'main', 'scala', '*.scala')].join(' ')
      end

      def self.exec(cmd)
        if $debug
          if Kernel.system(cmd)
            return
          else
            raise Spark::BuildError, cmd
          end
        end

        stdin, stdout, stderr, wait_thr = Open3.popen3(cmd)
        stdin.close
        stdout.close
        # stderr.close

        if wait_thr.value.success?
          puts ' ... OK'
        else
          raise Spark::BuildError, "'#{cmd}' \n failed: #{stderr}"
        end
      end

  end
end
