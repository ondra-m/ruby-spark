import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.RubyFixnum;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

/** Murmur hash 2.0.
 * 
 * The murmur hash is a relative fast hash function from
 * http://murmurhash.googlepages.com/ for platforms with efficient
 * multiplication.
 *
 * http://d3s.mff.cuni.cz/~holub/sw/javamurmurhash/
 *
 */

@JRubyClass(name="Spark::Digest::Murmur2")
public class Murmur2 extends RubyObject {

  public Murmur2(final Ruby ruby, RubyClass rubyClass) {
    super(ruby, rubyClass);
  }

  @JRubyMethod(required=1, optional=1, module=true)
  public static IRubyObject digest(ThreadContext context, IRubyObject self, IRubyObject[] args) {
    Ruby ruby = context.getRuntime();

    RubyString keyString = (RubyString)args[0];
    long seed;

    if(args.length > 1){
      RubyFixnum rb_seed = (RubyFixnum)args[1];
      seed = rb_seed.getLongValue();
    }
    else{
      seed = 0;
    }

    long hash = hash64(keyString.getBytes(), (int)keyString.length().getLongValue(), seed);

    RubyFixnum result = new RubyFixnum(ruby, hash);
    return result;
  }


  /** Generates 64 bit hash from byte array of the given length and seed.
   * 
   * @param data byte array to hash
   * @param length length of the array to hash
   * @param seed initial seed value
   * @return 64 bit hash of the given array
   */
  public static long hash64(final byte[] data, int length, long seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    long h = (seed&0xffffffffl)^(length*m);

    int length8 = length/8;

    for (int i=0; i<length8; i++) {
      final int i8 = i*8;
      long k =  ((long)data[i8+0]&0xff)      +(((long)data[i8+1]&0xff)<<8)
          +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
          +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
          +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);
      
      k *= m;
      k ^= k >>> r;
      k *= m;
      
      h ^= k;
      h *= m; 
    }
    
    switch (length%8) {
    case 7: h ^= (long)(data[(length&~7)+6]&0xff) << 48;
    case 6: h ^= (long)(data[(length&~7)+5]&0xff) << 40;
    case 5: h ^= (long)(data[(length&~7)+4]&0xff) << 32;
    case 4: h ^= (long)(data[(length&~7)+3]&0xff) << 24;
    case 3: h ^= (long)(data[(length&~7)+2]&0xff) << 16;
    case 2: h ^= (long)(data[(length&~7)+1]&0xff) << 8;
    case 1: h ^= (long)(data[length&~7]&0xff);
            h *= m;
    };
   
    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;

    return h;
  }

}
