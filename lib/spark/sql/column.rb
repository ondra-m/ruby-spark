module Spark
  module SQL
    class Column

      # =============================================================================
      # Creating

      def self.to_java(col)
        if col.is_a?(Column)
          col.jcolumn
        else
          from_name(col)
        end
      end

      def self.from_literal(literal)
        JSQLFunctions.lit(literal)
      end

      def self.from_name(name)
        JSQLFunctions.col(name)
      end


      # =============================================================================
      # Functions for virtual columns

      # Evaluates a list of conditions and returns one of multiple possible result expressions.
      # If {Column.otherwise} is not invoked, nil is returned for unmatched conditions.
      #
      # == Parameters:
      # condition:: a boolean {Column} expression
      # value:: a literal value, or a {Column} expression
      #
      # == Example:
      #   df.select(when(df['age'] == 2, 3).otherwise(4).alias("age")).collect()
      #   # [Row(age=3), Row(age=4)]
      #
      #   df.select(when(df.age == 2, df.age + 1).alias("age")).collect()
      #   # [Row(age=3), Row(age=nil)]
      #
      def self.when(condition, value)
        Column.new(JSQLFunctions).when(condition, value)
      end


      # =============================================================================
      # Initialized column

      attr_reader :jcolumn

      def initialize(jcolumn)
        @jcolumn = jcolumn
      end

      FUNC_OPERATORS = {
        '!' => 'not',
        '~' => 'negate',
        '-@' => 'negate'
      }

      BIN_OPERATORS = {
        '[]' => 'apply',
        '+' => 'plus',
        '-' => 'minus',
        '*' => 'multiply',
        '/' => 'divide',
        '%' => 'mod',
        '==' => 'equalTo',
        '!=' => 'notEqual',
        '<' => 'lt',
        '<=' => 'leq',
        '>' => 'gt',
        '>=' => 'geq',
        '&' => 'and',
        '|' => 'or',
        'like' => 'like',
        'starts_with' => 'startsWith',
        'ends_with' => 'endsWith',
        'bitwiseOR' => 'bitwiseOR',
        'bitwiseAND' => 'bitwiseAND',
        'bitwiseXOR' => 'bitwiseXOR',
      }

      UNARY_OPERATORS = {
        'asc' => 'asc',
        'desc' => 'desc',
        'is_null' => 'isNull',
        'is_not_null' => 'isNotNull'
      }

      FUNC_OPERATORS.each do |op, func|
        eval <<-METHOD
          def #{op}
            func_op('#{func}')
          end
        METHOD
      end

      BIN_OPERATORS.each do |op, func|
        eval <<-METHOD
          def #{op}(item)
            bin_op('#{func}', item)
          end
        METHOD
      end

      UNARY_OPERATORS.each do |op, func|
        eval <<-METHOD
          def #{op}
            unary_op('#{func}')
          end
        METHOD
      end

      # An expression that gets an item at position ordinal out of a list,
      # or gets an item by key out of a Hash.
      #
      # == Example:
      #   df.select(df.l.get_item(0), df.d.get_item("key")).show
      #   # +----+------+
      #   # |l[0]|d[key]|
      #   # +----+------+
      #   # |   1| value|
      #   # +----+------+
      #
      #   df.select(df.l[0], df.d["key"]).show
      #   # +----+------+
      #   # |l[0]|d[key]|
      #   # +----+------+
      #   # |   1| value|
      #   # +----+------+
      #
      def get_item(key)
        self[key]
      end

      # An expression that gets a field by name in a StructField.
      #
      # == Example:
      #   df.select(df.r.get_field("b")).show
      #   # +----+
      #   # |r[b]|
      #   # +----+
      #   # |   b|
      #   # +----+
      #
      #   df.select(df.r.a).show
      #   # +----+
      #   # |r[a]|
      #   # +----+
      #   # |   1|
      #   # +----+
      #
      def get_field(name)
        self[name]
      end

      # Return a {Column} which is a substring of the column.
      #
      # == Parameters:
      # start:: start position (Integer or Column)
      # length:: length of the substring (Integer or Column)
      #
      # == Example:
      #   df.select(df.name.substr(1, 3).alias("col")).collect
      #   # => [#<Row(col: "Ali")>, #<Row(col: "Bob")>]
      #
      def substr(start, length)
        if start.is_a?(Integer) && length.is_a?(Integer)
          new_jcolumn = jcolumn.substr(start, length)
        elsif start.is_a?(Column) && length.is_a?(Column)
          new_jcolumn = jcolumn.substr(start.jcolumn, length.jcolumn)
        else
          raise ArgumentError, "Unsupported type: #{start.class} and #{length.class}."
        end

        Column.new(new_jcolumn)
      end

      # A boolean expression that is evaluated to true if the value of this
      # expression is contained by the evaluated values of the arguments.
      #
      # == Example:
      #   df[df.name.isin("Bob", "Mike")].collect
      #   # => [#<Row(age: "5", name: "Bob")>]
      #
      #   df[df.age.isin(1, 2, 3)].collect
      #   # => [#<Row(age: "2", name: "Alice")>]
      #
      def isin(*cols)
        if cols.size == 1 && cols.first.is_a?(Array)
          cols = cols.first
        end

        cols = cols.map do |col|
          Column.from_literal(col)
        end

        new_jcolumn = jcolumn.isin(Spark.jb.to_seq(cols))
        Column.new(new_jcolumn)
      end

      # Returns this column aliased with a new name or names (in the case of expressions that
      # return more than one column, such as explode).
      #
      # == Example:
      #   df.select(df.age.alias("age2")).collect
      #   # => [#<Row(age2: "2")>, #<Row(age2: "5")>]
      #
      def alias(name)
        Column.new(jcolumn.as(name))
      end

      # Convert the column into type data_type.
      #
      # == Example:
      #   df.select(df.age.cast("string").alias('ages')).collect
      #   # => [#<Row(ages: "2")>, #<Row(ages: "5")>]
      #
      #   df.select(df.age.cast(StringType.new).alias('ages')).collect
      #   # => [#<Row(ages: "2")>, #<Row(ages: "5")>]
      #
      def cast(data_type)
        case data_type
        when String
          new_jcolumn = jcolumn.cast(data_type)
        when DataType
          jdata_type = JDataType.fromJson(data_type.json)
          new_jcolumn = jcolumn.cast(jdata_type)
        else
          raise ArgumentError, "Unsupported type: #{data_type.class}"
        end

        Column.new(new_jcolumn)
      end

      # A boolean expression that is evaluated to true if the value of this
      # expression is between the given columns.
      #
      # == Example:
      #   df.select(df.name, df.age.between(2, 4)).show
      #   # +-----+--------------------------+
      #   # | name|((age >= 2) && (age <= 4))|
      #   # +-----+--------------------------+
      #   # |Alice|                      true|
      #   # |  Bob|                     false|
      #   # +-----+--------------------------+
      #
      def between(lower, upper)
        (self >= lower) & (self <= upper)
      end

      # Evaluates a list of conditions and returns one of multiple possible result expressions.
      # If {Column.otherwise} is not invoked, nil is returned for unmatched conditions.
      #
      # == Parameters:
      # condition:: a boolean {Column} expression.
      # value:: a literal value, or a {Column} expression.
      #
      # == Example:
      #   df.select(df.name, Column.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show
      #   # +-----+--------------------------------------------------------+
      #   # | name|CASE WHEN (age > 4) THEN 1 WHEN (age < 3) THEN -1 ELSE 0|
      #   # +-----+--------------------------------------------------------+
      #   # |Alice|                                                      -1|
      #   # |  Bob|                                                       1|
      #   # +-----+--------------------------------------------------------+
      #
      def when(condition, value)
        unless condition.is_a?(Column)
          raise ArgumentError, "Condition must be a Column"
        end

        if value.is_a?(Column)
          value = value.jcolumn
        end
        new_jcolumn = jcolumn.when(condition.jcolumn, value)
        Column.new(new_jcolumn)
      end


      # Evaluates a list of conditions and returns one of multiple possible result expressions.
      # If {Column.otherwise} is not invoked, nil is returned for unmatched conditions.
      #
      # == Example:
      #   df.select(df.name, Column.when(df.age > 3, 1).otherwise(0)).show
      #   # +-----+---------------------------------+
      #   # | name|CASE WHEN (age > 3) THEN 1 ELSE 0|
      #   # +-----+---------------------------------+
      #   # |Alice|                                0|
      #   # |  Bob|                                1|
      #   # +-----+---------------------------------+
      #
      def otherwise(value)
        if value.is_a?(Column)
          value = value.jcolumn
        end

        new_jcolumn = jcolumn.otherwise(value)
        Column.new(new_jcolumn)
      end

      def over(*)
        raise Spark::NotImplemented
      end

      def method_missing(method, item)
        get_field(item)
      end

      def to_s
        "Column(\"#{jcolumn.toString}\")"
      end

      def inspect
        "#<#{to_s}>"
      end


      alias_method :as, :alias
      alias_method :slice, :substr
      alias_method :astype, :cast

      private

        def func_op(name)
          new_jcolumn = JSQLFunctions.__send__(name, jcolumn)
          Column.new(new_jcolumn)
        end

        def bin_op(name, item)
          if item.is_a?(Column)
            col = item.jcolumn
          else
            col = item
          end

          new_jcolumn = jcolumn.__send__(name, col)
          Column.new(new_jcolumn)
        end

        def unary_op(name)
          new_jcolumn = jcolumn.__send__(name)
          Column.new(new_jcolumn)
        end


    end
  end
end
