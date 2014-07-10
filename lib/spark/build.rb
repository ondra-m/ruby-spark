module Spark
  module Build

    def self.spark(target=nil)
      dir = Dir.mktmpdir
      target ||= File.join(root, 'target')

      begin
        system(get_ivy.call(dir, 'ivy.jar'))
        system(get_spark.call(dir, 'ivy.jar'))
        FileUtils.mkdir_p(target)
        FileUtils.mv(Dir.glob(File.join(dir, 'spark', '*')), target)
      rescue
      ensure
        FileUtils.remove_entry(dir)
      end
    end

    def self.ext(spark=nil)
      spark ||= File.join(root, 'target')

      begin
        system(compile_ext.call(spark))
      rescue
      end
    end

    private

      def self.root
        @root = File.expand_path(File.dirname(__FILE__) + '/../../')
      end

      def self.get_ivy
        @ivy ||= Proc.new{|dir, ivy| ["curl", 
                                      "-o #{dir}/#{ivy}", 
                                      "http://search.maven.org/remotecontent\?filepath\=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar"].join(" ")}
      end

      def self.get_spark
        @spark ||= Proc.new{|dir, ivy| ["java",
                                        "-jar #{dir}/#{ivy}",
                                        "-dependency org.apache.spark spark-core_2.10 1.0.0",
                                        "-retrieve \"#{dir}/spark/[artifact]-[revision](-[classifier]).[ext]\""].join(" ")}
      end

      def self.compile_ext
        @ext ||= Proc.new{|classpath| ["scalac",
                                       "-d #{root}/target/ruby-spark.jar",
                                       "-classpath \"#{classpath}/*\"",
                                       "#{root}/src/main/scala/RubyRDD.scala"].join(" ")}
      end

  end
end
