package de.tu-dresden.el2db

import org.scalatra._

class EL2DBWeb extends ScalatraServlet {

  get("/") {
    views.html.hello()
  }

}
