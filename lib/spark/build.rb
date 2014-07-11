module Spark
  module Build

    def self.spark(target=nil)
      dir = Dir.mktmpdir

      begin
        system(get_ivy.call(dir, 'ivy.jar'))
        system(get_spark.call(dir, 'ivy.jar'))
        FileUtils.mkdir_p(Spark.target_dir)
        FileUtils.mv(Dir.glob(File.join(dir, 'spark', '*')), Spark.target_dir)
      rescue
        raise Spark::BuildError, "Cannot build Spark."
      ensure
        FileUtils.remove_entry(dir)
      end
    end

    def self.ext(spark=nil)
      spark ||= Spark.target_dir

      begin
        system(compile_ext.call(spark))
      rescue
      end
    end

    private

      def self.get_ivy
        Proc.new{|dir, ivy| ["curl", 
                             "-o", File.join(dir, ivy),
                             "http://search.maven.org/remotecontent\?filepath\=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar"].join(" ")}
      end

      def self.get_spark
        Proc.new{|dir, ivy| ["java",
                             "-jar", File.join(dir, ivy),
                             "-dependency org.apache.spark spark-core_2.10 1.0.0",
                             "-retrieve \"", File.join(dir, "spark", "[artifact]-[revision](-[classifier]).[ext]"), "\""].join(" ")}
      end

      def self.compile_ext
        Proc.new{|classpath| ["scalac",
                              "-d", Spark.ruby_spark_jar,
                              "-classpath \"", File.join(classpath, '*'), "\"",
                              File.join(Spark.root, "src", "main", "scala", "*.scala")].join(" ")}
      end

  end
end
