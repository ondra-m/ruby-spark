package org.apache.spark.ui.ruby

// import javax.servlet.http.HttpServletRequest

// import scala.xml.Node

// import org.apache.spark.ui.{WebUIPage, UIUtils}
// import org.apache.spark.util.Utils

// private[ui] class RubyPage(parent: RubyTab, rbConfig: Array[Tuple2[String, String]]) extends WebUIPage("") {

//   def render(request: HttpServletRequest): Seq[Node] = {
//     val content = UIUtils.listingTable(header, row, rbConfig)
//     UIUtils.headerSparkPage("Ruby Config", content, parent)
//   }

//   private def header = Seq(
//     "Number"
//   )

//   private def row(keyValue: (String, String)): Seq[Node] = {
//     // scalastyle:off
//     keyValue match {
//       case (key, value) =>
//         <tr>
//           <td>{key}</td>
//           <td>{value}</td>
//         </tr>
//     }
//     // scalastyle:on
//   }
// }

class RubyPage {}
