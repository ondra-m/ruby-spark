module Spark
  module Library

    def autoload(klass, location, import=true)
      if import
        @for_importing ||= []
        @for_importing << klass
      end

      super(klass, location)
    end

    def autoload_without_import(klass, location)
      autoload(klass, location, false)
    end

    def import(to=Object)
      @for_importing.each do |klass|
        to.const_set(klass, const_get(klass))
      end
      nil
    end

  end
end
