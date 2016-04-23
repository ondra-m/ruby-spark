require 'simplecov'
SimpleCov.start

$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
require 'ruby-spark'
require 'generator'

# Loading
Spark.load_lib
Spark.jb.import_all_test
Spark::Mllib.import

# Keep it on method because its called from config test
def spark_start
  Spark.logger.disable
  Spark.config do
    set 'spark.ruby.serializer.batch_size', 100
  end
  $sc = Spark.start
  $sql = Spark.start_sql
end

def windows?
  RbConfig::CONFIG['host_os'] =~ /mswin|mingw/
end

RSpec.configure do |config|
  config.default_formatter = 'doc'
  config.color = true
  config.tty   = true

  config.before(:suite) do
    spark_start
  end
  config.after(:suite) do
    Spark.stop
  end
end
