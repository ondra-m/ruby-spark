require 'open3'

module Spark
  module Build

    def self.spark(target=nil)
      dir = Dir.mktmpdir

      begin
        puts "Building ivy"
        exec(get_ivy.call(dir, 'ivy.jar'))
        puts "Building spark"
        exec(get_spark.call(dir, 'ivy.jar'))
        puts "Moving files"
        FileUtils.mkdir_p(Spark.target_dir)
        FileUtils.mv(Dir.glob(File.join(dir, 'spark', '*')), Spark.target_dir)
      rescue Exception => e
        raise Spark::BuildError, "Cannot build Spark. #{e}"
      ensure
        FileUtils.remove_entry(dir)
      end
    end

    def self.ext(spark=nil)
      spark ||= Spark.target_dir

      begin
        puts "Building ruby-spark extension"
	exec(compile_ext.call(spark))
      rescue Exception => e
        raise Spark::BuildError, "Cannot build ruby-spark extension. #{e}", e.backtrace
      end
    end

    private

      def self.check_status
        raise StandardError unless $?.success?
      end

      def self.get_ivy
        Proc.new{|dir, ivy| ["curl",
                             "-o", File.join(dir, ivy),
                             "http://search.maven.org/remotecontent\?filepath\=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar"].join(" ")}
      end

      def self.get_spark
        Proc.new{|dir, ivy| ["java",
                             "-jar", File.join(dir, ivy),
                             "-dependency org.apache.spark spark-core_2.10 1.0.0",
                             "-retrieve", "\"#{File.join(dir, "spark", "[artifact]-[revision](-[classifier]).[ext]")}\""].join(" ")}
      end

      def self.compile_ext
        Proc.new{|classpath| ["scalac",
                              "-d", Spark.ruby_spark_jar,
                              "-classpath", "\"#{File.join(classpath, '*')}\"",
                              File.join(Spark.root, "src", "main", "scala", "*.scala")].join(" ")}
      end

      def self.exec(cmd)
	Open3.popen3(cmd) do |stdin, stdout, stderr, wait_thr|
	  exit_status = wait_thr.value
	  unless exit_status.success?
	    err = stderr.read
	    raise Spark::BuildError, "'#{cmd}' \n failed: #{err}"
	  end
	end
      end

  end
end
