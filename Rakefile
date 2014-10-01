#-*- mode: ruby -*-

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new

task default: :spec
task test:    :spec

def java?
  RUBY_PLATFORM =~ /java/
end

if java?
  require "rake/javaextensiontask"
  Rake::JavaExtensionTask.new("ruby_java") do |ext|
    ext.name = "ruby_spark_ext"
  end
else
  require "rake/extensiontask"
  Rake::ExtensionTask.new("ruby_c") do |ext|
    ext.name = "ruby_spark_ext"
  end
end


task :clean do
  Dir['lib/*.{jar,o,so}'].each do |path|
    puts "Deleting #{path} ..."
    File.delete(path)
  end
  FileUtils.rm_rf('./pkg')
  FileUtils.rm_rf('./tmp')
end
