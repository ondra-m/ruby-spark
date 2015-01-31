class SparkFiles

  class << self
    attr_accessor :root_directory
  end

  def self.get(file_name)
    File.join(root_directory, file_name)
  end

  def self.get_content(file_name)
    File.read(get(file_name))
  end

end
