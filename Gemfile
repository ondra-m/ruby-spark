source 'https://rubygems.org'

gemspec

gem 'sourcify', '0.6.0.rc4'
gem 'method_source'
gem 'commander'
gem 'pry'
gem 'nio4r'
gem 'distribution'
# gem 'thor'
# gem 'algorithms'
# gem 'croupier', '>= 2.0.0.beta'
# gem 'yaml'
# gem 'childprocess'

platform :mri do
  gem 'rjb'
  gem 'msgpack'
  gem 'oj'
end

platform :jruby do
  gem 'msgpack-jruby', :require => 'msgpack'
end

group :development do
  gem 'benchmark-ips'
  gem 'rspec'
  gem 'rake-compiler'
  gem 'guard'
  gem 'guard-rspec'
end

group :test do
  gem 'simplecov', :require => false
end
