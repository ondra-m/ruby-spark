import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

public class RubySparkExtService implements BasicLibraryService
{
  public boolean basicLoad(final Ruby ruby) throws java.io.IOException {

    RubyModule sparkModule = ruby.defineModule("Spark");
    RubyModule sparkDigestModule = sparkModule.defineModuleUnder("Digest");
    RubyClass  sparkDigestMurmur2Class = sparkDigestModule.defineClassUnder("Murmur2", ruby.getObject(), sparkDigestMurmur2Allocator);

    sparkDigestModule.defineAnnotatedMethods(Digest.class);
    sparkDigestMurmur2Class.defineAnnotatedMethods(Murmur2.class);

    return true;
  }

  public static ObjectAllocator sparkDigestMurmur2Allocator = new ObjectAllocator() {
    public IRubyObject allocate(Ruby ruby, RubyClass rubyClass) {
      return new Murmur2(ruby, rubyClass);
    }
  };

}
