#-*- mode: ruby -*-
group_id "cvut"
artifact_id "ruby-spark"
version "0.0.1"

#gemspec :include_jars => true

gemfile

jarfile

#gemspec :source => 'src/main/scala'

properties( 'jruby.versions' => ['1.6.8','1.7.4', '1.7.13'].join(','),
            'jruby.modes' => ['1.8', '1.9', '2.0', '2.1'].join(','),
            # just lock the versions
            'jruby.version' => '1.7.13',
            'jruby.plugins.version' => '1.0.3',
	    'scala.version' => '2.10.4',
            'tesla.dump.pom' => 'pom.xml',
            'tesla.dump.readonly' => true )

plugin :compiler, '3.1' do
  execute_goals( :testCompile, :phase => 'test-compile' )
end

plugin 'org.scala-tools:maven-scala-plugin', '2.15.2' do
  execute_goals( :compile, :phase => 'compile' )
end
