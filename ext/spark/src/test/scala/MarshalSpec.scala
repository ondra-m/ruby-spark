package org.apache.spark.api.ruby.marshal

import org.scalatest._


import org.apache.spark.api.ruby.marshal._

class MarshalSpec extends FunSpec with Matchers {

  // ====================================================================================
  // Load

  describe("Marshal.load"){
    describe("single value"){
      it("int"){
        val data = 1
        val serialized = Array[Byte](4, 8, 105, 6)

        Marshal.load(serialized) should equal(data)
      }

      it("double"){
        val data = 1.2
        val serialized = Array[Byte](4, 8, 102, 8, 49, 46, 50)

        Marshal.load(serialized) should equal(data)
      }
    }

    describe("array"){
      it("ints"){
        val data = Array(1, 2, 3, 4, 5)
        val serialized = Array[Byte](4, 8, 91, 10, 105, 6, 105, 7, 105, 8, 105, 9, 105, 10)

        Marshal.load(serialized) should equal(data)
      }

      it("doubles"){
        val data = Array(1.1, 2.2, 3.3)
        val serialized = Array[Byte](4, 8, 91, 8, 102, 8, 49, 46, 49, 102, 8, 50, 46, 50, 102, 8, 51, 46, 51)

        Marshal.load(serialized) should equal(data)
      }
    }
  }

  // ====================================================================================
  // Dump

  describe("Marshal.dump"){
    describe("single value"){
      it("int"){
        val data = 1
        val serialized = Array(4, 8, 105, 6)

        Marshal.dump(data) should equal(serialized)
      }

      it("double"){
        val data = 1.2
        val serialized = Array(4, 8, 102, 8, 49, 46, 50)

        Marshal.dump(data) should equal(serialized)
      }
    }

    describe("array"){
      it("ints"){
        val data = Array(1, 2, 3, 4, 5)
        val serialized = Array(4, 8, 91, 10, 105, 6, 105, 7, 105, 8, 105, 9, 105, 10)

        Marshal.dump(data) should equal(serialized)
      }

      it("doubles"){
        val data = Array(1.1, 2.2, 3.3)
        val serialized = Array(4, 8, 91, 8, 102, 8, 49, 46, 49, 102, 8, 50, 46, 50, 102, 8, 51, 46, 51)

        Marshal.dump(data) should equal(serialized)
      }
    }
  }

}
