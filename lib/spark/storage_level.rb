# Necessary libraries
Spark.load_lib

module Spark
  class StorageLevel

    def self.reload
      return if @reloaded
      reload!
      @reloaded = true
    end

    def self.reload!
      self.const_set(:NONE,                  JStorageLevel.NONE)
      self.const_set(:DISK_ONLY,             JStorageLevel.DISK_ONLY)
      self.const_set(:DISK_ONLY_2,           JStorageLevel.DISK_ONLY_2)
      self.const_set(:MEMORY_ONLY,           JStorageLevel.MEMORY_ONLY)
      self.const_set(:MEMORY_ONLY_SER,       JStorageLevel.MEMORY_ONLY_SER)
      self.const_set(:MEMORY_ONLY_2,         JStorageLevel.MEMORY_ONLY_2)
      self.const_set(:MEMORY_ONLY_SER_2,     JStorageLevel.MEMORY_ONLY_SER_2)
      self.const_set(:MEMORY_AND_DISK,       JStorageLevel.MEMORY_AND_DISK)
      self.const_set(:MEMORY_AND_DISK_2,     JStorageLevel.MEMORY_AND_DISK_2)
      self.const_set(:MEMORY_AND_DISK_SER,   JStorageLevel.MEMORY_AND_DISK_SER)
      self.const_set(:MEMORY_AND_DISK_SER_2, JStorageLevel.MEMORY_AND_DISK_SER_2)
      self.const_set(:OFF_HEAP,              JStorageLevel.OFF_HEAP)
    end

    def self.java_get(arg)
      reload

      if arg.is_a?(String)
        const_get(arg.upcase)
      else
        arg
      end
    end

  end
end
