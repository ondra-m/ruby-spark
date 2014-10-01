#ifndef MURMUR_INCLUDED
#define MURMUR_INCLUDED

#include "ruby.h"

VALUE method_portable_hash(int argc, VALUE *argv, VALUE klass);
VALUE method_murmur2_digest(int argc, VALUE *argv, VALUE klass);

#endif
