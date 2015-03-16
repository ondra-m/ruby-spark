package org.apache.spark.api.ruby

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.ruby.marshal._


/* =================================================================================================
 * object RubySerializer
 * =================================================================================================
 */
object RubySerializer {

  /**
   * Convert an RDD of serialized Ruby objects to RDD of objects, that is usable in Java.
   */
  def rubyToJava(rbRDD: JavaRDD[Array[Byte]], batched: Boolean): JavaRDD[Any] = {
    rbRDD.rdd.mapPartitions { iter =>
      iter.flatMap { item =>
        val obj = Marshal.load(item)
        if(batched){
          obj.asInstanceOf[Array[_]]
        }
        else{
          Seq(item)
        }
      }
    }.toJavaRDD()
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Ruby objects, that is usable by Ruby.
   */
  def javaToRuby(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    jRDD.rdd.mapPartitions { iter => new IterableMarshaller(iter) }
  }

}
