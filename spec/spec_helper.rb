$:.unshift File.dirname(__FILE__) + '/../lib'
require "ruby-spark"

Example = Struct.new(:function, :task, :result)

RSpec.configure do |config|
  config.color = true
  config.tty = true

  config.before(:suite) do
    Spark.disable_log
    $sc = Spark::Context.new(app_name: "RubySpark", master: "local[*]")
  end
  config.after(:suite) do
    # Spark.destroy_all
  end
end
