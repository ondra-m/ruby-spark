#-*- mode: ruby -*-

require "bundler/gem_tasks"
# require "maven/ruby/tasks"

# task :default => [ :test ]

# desc "run all tests"
# task :all do
#   maven.verify
# end

namespace :spark do
  desc "Get a SPARK"
  task :get do

    dir = Dir.mktmpdir
    gem_root = File.dirname(__FILE__)

    ivy = ["curl", 
           "-o #{dir}/ivy.jar", 
           "http://search.maven.org/remotecontent\?filepath\=org/apache/ivy/ivy/2.3.0/ivy-2.3.0.jar"].join(" ")
    java = ["java",
            "-jar #{dir}/ivy.jar",
            "-dependency org.apache.spark spark-core_2.10 1.0.0",
            "-retrieve \"#{gem_root}/include/[artifact]-[revision](-[classifier]).[ext]\""].join(" ")

    begin
      if system(ivy)
        system(java)
      end
    ensure
      FileUtils.remove_entry(dir)
    end

  end

  desc "Build a ruby extension"
  task :build_ext do
    
  end
end
