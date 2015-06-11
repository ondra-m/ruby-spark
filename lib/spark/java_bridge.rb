module Spark
  module JavaBridge

    autoload :Base,  'spark/java_bridge/base'
    autoload :JRuby, 'spark/java_bridge/jruby'
    autoload :RJB,   'spark/java_bridge/rjb'

    include Spark::Helper::System

    def self.init(*args)
      if jruby?
        klass = JRuby
      else
        klass = RJB
      end

      klass.new(*args)
    end

  end
end
