class Hash
  
  # Destructively convert all keys to strings.
  if !method_defined?(:stringify_keys)
    def stringify_keys!
      transform_keys!{ |key| key.to_s }
    end
  end

  # Destructively convert all keys using the block operations.
  # Same as transform_keys but modifies +self+.
  if !method_defined?(:transform_keys!)
    def transform_keys!
      keys.each do |key|
        self[yield(key)] = delete(key)
      end
      self
    end
  end

end
