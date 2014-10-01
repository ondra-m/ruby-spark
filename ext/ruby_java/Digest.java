import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyClass;
import org.jruby.RubyString;
import org.jruby.RubyFixnum;
import org.jruby.anno.JRubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@JRubyModule(name="Spark::Digest")
public class Digest extends RubyObject{

  // Have to be the same as in C extension
  final static long PORTABLE_HASH_SEED = 16154832;

  public Digest(final Ruby ruby, RubyClass rubyClass) {
    super(ruby, rubyClass);
  }

  @JRubyMethod(module=true)
  public static IRubyObject portable_hash(ThreadContext context, IRubyObject self, IRubyObject arg) {
    Ruby ruby = self.getRuntime();

    RubyString keyString = (RubyString)arg;

    long hash = Murmur2.hash64(keyString.getBytes(), (int)keyString.length().getLongValue(), PORTABLE_HASH_SEED);

    RubyFixnum result = new RubyFixnum(ruby, hash);

    return result;
  }

}

