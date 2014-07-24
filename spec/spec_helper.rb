$:.unshift File.dirname(__FILE__) + '/../lib'
require "ruby-spark"

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
  config.run_all_when_everything_filtered = true
  config.color_enabled = true
  config.formatter     = 'documentation'

  config.before(:suite) do
    $sc = Spark::Context.new(app_name: "RubySpark", master: "local")
  end
  config.after(:suite) do
  end
end
