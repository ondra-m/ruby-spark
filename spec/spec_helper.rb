require 'simplecov'
SimpleCov.start

$:.unshift File.dirname(__FILE__) + '/../lib'
require "ruby-spark"
require "generator"


RSpec.configure do |config|
  config.default_formatter = "doc"
  config.color = true
  config.tty   = true

  config.before(:suite) do
    Spark.disable_log
    $sc = Spark::Context.new("spark.ruby.parallelize_strategy" => "deep_copy")
  end
  config.after(:suite) do
    Spark.destroy_workers
  end
end
