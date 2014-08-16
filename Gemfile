source 'https://rubygems.org'

gemspec

gem 'sourcify', '0.6.0.rc4'
gem 'thor'
gem 'pry'
# gem 'yaml'
# gem 'childprocess'

platform :mri do
  gem 'rjb'
  gem 'msgpack'
end

platform :jruby do
  gem 'msgpack-jruby', :require => 'msgpack'
end

group :development do
  gem 'rspec'
  gem 'guard'
  gem 'guard-rspec'
end

group :test do
  gem 'simplecov', :require => false
end
