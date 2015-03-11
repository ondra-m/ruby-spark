module Spark
  module Mllib
    ##
    # LabeledPoint
    #
    # The features and labels of a data point.
    #
    # == Parameters:
    # label::
    #   Label for this data point.
    #
    # features::
    #   Vector of features for this point
    #
    class LabeledPoint

      attr_reader :label, :features

      def initialize(label, features)
        @label = label.to_f
        @features = Spark::Mllib::Vectors.to_vector(features)
      end

      def self.from_java(object)
        LabeledPoint.new(
          object.label,
          Spark.jb.java_to_ruby(object.features)
        )
      end

      def marshal_dump
        [@label, @features]
      end

      def marshal_load(array)
        initialize(array[0], array[1])
      end

    end
  end
end
