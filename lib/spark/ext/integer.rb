class Integer
  if !const_defined?(:MAX)
    MAX = 1 << (1.size * 8 - 2) - 1
  end

  if !const_defined?(:MIN)
    MIN = -MAX - 1
  end
end
