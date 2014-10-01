#include "ruby.h"
#include "murmur.h"


VALUE SparkModule;
VALUE SparkDigestModule;
VALUE SparkDigestMurmur2Class;


void Init_ruby_spark_ext()
{
  SparkModule             = rb_define_module("Spark");
  SparkDigestModule       = rb_define_module_under(SparkModule, "Digest");
  SparkDigestMurmur2Class = rb_define_class_under(SparkDigestModule, "Murmur2", rb_cObject);

  rb_define_singleton_method(SparkDigestModule, "portable_hash", method_portable_hash, -1);
  rb_define_singleton_method(SparkDigestMurmur2Class, "digest", method_murmur2_digest, -1);
}
