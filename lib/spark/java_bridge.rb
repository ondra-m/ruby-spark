module Spark
  module JavaBridge

    autoload :Base,  "spark/java_bridge/base"
    autoload :JRuby, "spark/java_bridge/jruby"
    autoload :RJB,   "spark/java_bridge/rjb"

    include Spark::Helper::System

    def self.get
      if jruby?
        JRuby
      else
        RJB
      end
    end

  end
end
