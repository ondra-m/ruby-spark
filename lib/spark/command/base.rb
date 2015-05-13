##
# Spark::Command::Base
#
# Parent for all commands (Map, FlatMap, Sort, ...)
#
class Spark::Command::Base

  DEFAULT_VARIABLE_OPTIONS = {
    type: Hash,
    function: true
  }

  def initialize(*args)
    settings.variables.each do |name, options|
      instance_variable_set("@#{name}", args.shift)
    end
  end

  def to_s
    self.class.name.split('::').last
  end

  def self.error(message)
    raise Spark::CommandError, message
  end

  def error(message)
    self.class.error(message)
  end

  def log(message=nil)
    $stdout.puts %{==> #{Time.now.strftime("%H:%M:%S")} [#{self.class.name}] #{message}}
    $stdout.flush
  end


  # ===============================================================================================
  # Methods called during class loading
  # This is not nicer way but these methods set/get classes variables for child

  # Settings for command (variables)
  def self.settings
    init_settings
    class_variable_get(:@@settings)
  end

  def settings
    self.class.settings
  end

  # Init empty settings
  def self.init_settings
    if !class_variable_defined?(:@@settings)
      struct = Struct.new(:variables)

      class_variable_set(:@@settings, struct.new)
      settings.variables = {}
    end
  end

  # New variable for command
  #
  # == Example:
  #
  #   class Map < Spark::Command::Base
  #     variable :map_function
  #   end
  #
  #   command = Map.new(1)
  #
  #   command.instance_variables
  #   # => [:@map_function]
  #   command.instance_variable_get(:@map_function)
  #   # => 1
  #
  def self.variable(name, options={})
    if settings.variables.has_key?(name)
      error "Function #{name} already exist."
    end

    settings.variables[name] = DEFAULT_VARIABLE_OPTIONS.merge(options)
  end


  # ===============================================================================================
  # Executing methods

  # Execute command for data and split index
  def execute(iterator, split_index)
    # Implemented on Base but can be override
    before_run

    # Run has to be implemented on child
    if iterator.is_a?(Enumerator::Lazy) && respond_to?(:lazy_run)
      return lazy_run(iterator, split_index)
    end

    iterator = iterator.to_a
    run(iterator, split_index)
  end

  def prepared?
    !!@prepared
  end

  # This is called before execution. Executing will be stopped if
  # some command contains error (e.g. badly serialized lambda).
  #
  # == What is doing?
  # * evaluate lambda
  # * evaluate method
  # * make new lambda
  #
  def prepare
    return if prepared?

    to_function = settings.variables.select {|_, options| options[:function]}
    to_function.each do |name, options|
      name = "@#{name}"
      data = instance_variable_get(name)

      case data[:type]
      when 'proc'
        result = eval(data[:content])
      when 'symbol'
        result = lambda(&data[:content])
      when 'method'
        # Method must me added to instance not Class
        instance_eval(data[:content])
        # Method will be available as Proc
        result = lambda(&method(data[:name]))
      end

      instance_variable_set(name, result)
    end

    @prepared = true
  end

  # This method is called before every execution.
  def before_run
  end


  # ===============================================================================================
  # Bound objects

  attr_accessor :__objects__

  def method_missing(method, *args, &block)
    if __objects__ && __objects__.has_key?(method)
      return __objects__[method]
    end

    super
  end

end
