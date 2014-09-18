class Object
  
  if !method_defined?(:deep_copy)
    def deep_copy
      Marshal.load(Marshal.dump(self))
    end
  end

end
