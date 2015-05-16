# coding: utf-8

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'spark/version'

Gem::Specification.new do |spec|
  spec.name          = 'ruby-spark'
  spec.version       = Spark::VERSION
  spec.authors       = ['Ondřej Moravčík']
  spec.email         = ['moravcik.ondrej@gmail.com']
  spec.summary       = %q{Ruby wrapper for Apache Spark}
  spec.description   = %q{}
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  if RUBY_PLATFORM =~ /java/
    spec.platform = 'java'

    extensions = ['ext/ruby_java/extconf.rb']
  else
    extensions = ['ext/ruby_c/extconf.rb']

    spec.add_dependency 'rjb'
  end

  spec.extensions = extensions
  spec.required_ruby_version = '>= 2.0'

  spec.requirements << 'java, scala'

  spec.add_dependency 'sourcify', '0.6.0.rc4'
  spec.add_dependency 'method_source'
  spec.add_dependency 'commander'
  spec.add_dependency 'pry'
  spec.add_dependency 'nio4r'
  spec.add_dependency 'distribution'

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake'
end
