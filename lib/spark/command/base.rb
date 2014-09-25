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

  def self.error(message)
    raise Spark::CommandError, message
  end

  def error(message)
    self.class.error(message)
  end

  def log(message=nil)
    $stdout.puts %{==> [#{Time.now.strftime("%H:%M")}] [#{self.class.name}] #{message}}
    $stdout.flush
  end


  # ===============================================================================================
  # Methods called after during class loading
  # This is not nicer way but these methods set/get classes variables for child

  def self.settings
    init_settings
    class_variable_get(:@@settings)
  end

  def self.init_settings
    if !class_variable_defined?(:@@settings)
      struct = Struct.new(:variables)

      class_variable_set(:@@settings, struct.new)
      settings.variables = {}
    end
  end

  def settings
    self.class.settings
  end

  def self.variable(name, options={})
    if settings.variables.has_key?(name)
      error "Function #{name} already exist."
    end

    settings.variables[name] = DEFAULT_VARIABLE_OPTIONS.merge(options)
  end


  # ===============================================================================================
  # Executing methods

  def execute(iterator, split_index)
    # Implemented on Base but can be override
    before_run

    # Run has to be implemented on child
    if iterator.is_a?(Enumerator) && respond_to?(:run_with_enum)
      return run_with_enum(iterator, split_index)
    end

    iterator = iterator.to_a
    run(iterator, split_index)
  end

  def initialized?
    !!@initialized
  end

  # This is called before every executing
  #
  # == What is doing?
  # * evaluting function (now it is just a string)
  #
  def before_run
    return if initialized?

    to_function = settings.variables.select {|_, options| options[:function]}
    to_function.each do |name, options|
      name = "@#{name}"
      data = instance_variable_get(name)

      case data[:type]
      when "proc"
        result = eval(data[:content])
      when "method"
        # Method must me added to instance not Class
        instance_eval(data[:content])
        # Method will be available as Proc
        result = lambda(&method(data[:name]))
      end

      instance_variable_set(name, result)
    end

    @initialized = true
  end

  def enum
    Enumerator.new
  end

end
