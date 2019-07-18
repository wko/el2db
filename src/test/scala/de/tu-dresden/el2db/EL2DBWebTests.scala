package de.tu-dresden.el2db

import org.scalatra.test.scalatest._

class EL2DBWebTests extends ScalatraFunSuite {

  addServlet(classOf[EL2DBWeb], "/*")

  test("GET / on EL2DBWeb should return status 200") {
    get("/") {
      status should equal (200)
    }
  }

}
