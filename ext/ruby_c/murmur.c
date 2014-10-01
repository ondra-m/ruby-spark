#include "murmur.h"

#if defined(_MSC_VER)
#define BIG_CONSTANT(x) (x)
#else
#define BIG_CONSTANT(x) (x##LLU)
#endif

/*-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby
//
// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment 
// and endian-ness issues if used across multiple platforms.
//
// 64-bit hash for 64-bit platforms
*/

uint64_t MurmurHash64A(const void * key, int len, uint64_t seed)
{
  const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t * data = (const uint64_t *)key;
  const uint64_t * end = data + (len/8);

  while(data != end)
  {
    uint64_t k = *data++;

    k *= m; 
    k ^= k >> r; 
    k *= m; 
    
    h ^= k;
    h *= m; 
  }

  const unsigned char * data2 = (const unsigned char*)data;

  switch(len & 7)
  {
  case 7: h ^= ((uint64_t) data2[6]) << 48;
  case 6: h ^= ((uint64_t) data2[5]) << 40;
  case 5: h ^= ((uint64_t) data2[4]) << 32;
  case 4: h ^= ((uint64_t) data2[3]) << 24;
  case 3: h ^= ((uint64_t) data2[2]) << 16;
  case 2: h ^= ((uint64_t) data2[1]) << 8;
  case 1: h ^= ((uint64_t) data2[0]);
          h *= m;
  };
 
  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
} 

/* 64-bit hash for 32-bit platforms */

uint64_t MurmurHash64B(const void * key, int len, uint64_t seed)
{
  const uint32_t m = 0x5bd1e995;
  const int r = 24;

  uint32_t h1 = ((uint32_t) seed) ^ len;
  uint32_t h2 = ((uint32_t) (seed >> 32));

  const uint32_t * data = (const uint32_t *)key;

  while(len >= 8)
  {
    uint32_t k1 = *data++;
    k1 *= m; k1 ^= k1 >> r; k1 *= m;
    h1 *= m; h1 ^= k1;
    len -= 4;

    uint32_t k2 = *data++;
    k2 *= m; k2 ^= k2 >> r; k2 *= m;
    h2 *= m; h2 ^= k2;
    len -= 4;
  }

  if(len >= 4)
  {
    uint32_t k1 = *data++;
    k1 *= m; k1 ^= k1 >> r; k1 *= m;
    h1 *= m; h1 ^= k1;
    len -= 4;
  }

  switch(len)
  {
  case 3: h2 ^= ((unsigned char*)data)[2] << 16;
  case 2: h2 ^= ((unsigned char*)data)[1] << 8;
  case 1: h2 ^= ((unsigned char*)data)[0];
      h2 *= m;
  };

  h1 ^= h2 >> 18; h1 *= m;
  h2 ^= h1 >> 22; h2 *= m;
  h1 ^= h2 >> 17; h1 *= m;
  h2 ^= h1 >> 19; h2 *= m;

  uint64_t h = h1;

  h = (h << 32) | h2;

  return h;
}



// ================================================================================================
// Ruby methods

#define PORTABLE_HASH_SEED 16154832


VALUE murmur2_digest(VALUE rb_str, uint64_t seed)
{
  StringValue(rb_str);

  void * key = RSTRING_PTR(rb_str);
  long   len = RSTRING_LEN(rb_str);

  uint64_t result = MurmurHash64A(key, len, seed);

  return LONG2FIX(result);
}

// ------------------------------------------------------------------------------------------------
// Spark::Digest::Murmur2.digest

VALUE method_murmur2_digest(int argc, VALUE *argv, VALUE klass)
{
  if(argc == 0 || argc > 2){
    rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
  }

  uint64_t seed = (argc == 1 ? 0 : NUM2UINT(argv[1]));

  return murmur2_digest(argv[0], seed);
}

// ------------------------------------------------------------------------------------------------
// Spark::Digest.portable_hash

VALUE method_portable_hash(int argc, VALUE *argv, VALUE klass)
{
  if(argc != 1){
    rb_raise(rb_eArgError, "wrong number of arguments (%d for 1)", argc);
  }

  return murmur2_digest(argv[0], PORTABLE_HASH_SEED);
}
