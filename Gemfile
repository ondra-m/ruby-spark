source 'https://rubygems.org'

gemspec

gem 'sourcify', '0.6.0.rc4'
gem 'method_source'
gem 'commander'
gem 'pry'
gem 'nio4r'
gem 'distribution'

platform :mri do
  gem 'rjb'
  gem 'msgpack'
  gem 'oj'
  gem 'narray'
end

platform :jruby do
  gem 'msgpack-jruby', require: 'msgpack'

  # NameError: no constructorfor arguments (org.jruby.RubyFixnum,org.jruby.RubyFixnum,org.jruby.RubyFixnum,org.jruby.RubyFixnum,org.jruby.RubyFixnum,org.jruby.RubyFixnum,org.joda.time.chrono.GJChronology) on Java::OrgJodaTime::DateTime
  # gem 'mdarray'
end

group :stats do
  # gem 'nmatrix'
  # gem 'statsample'
  # gem 'statsample-glm'
  # gem 'statsample-timeseries'
  # gem 'statistics2'
  # gem 'statsample-optimization' # libgsl0-dev
  # gem 'narray'
  # gem 'gsl-nmatrix'
end

group :development do
  gem 'benchmark-ips'
  gem 'rspec'
  gem 'rake-compiler'
  gem 'guard'
  gem 'guard-rspec'
  gem 'listen'
end

group :test do
  gem 'simplecov', require: false
end
