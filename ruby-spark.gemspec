#-*- mode: ruby -*-

Gem::Specification.new do |s|
  s.name = 'ruby-spark'
  s.version = "0.0.1"
  s.author = 'Ondrej Moravcik'
  s.email = [ 'moravon4@fit.cvut.cz' ]
  s.summary = 'Ruby wrapper for Spark'
  s.description = ''
  s.requirements << "jar org.apache.spark:spark-core_2.10, 1.0.0"
  s.files = Dir['lib/**']
end

# vim: syntax=Ruby
