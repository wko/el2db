package de.tu_dresden.el2db


import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import org.phenoscape.scowl._
import de.tu_dresden.OntologyUtils
import org.scalatest._
import org.semanticweb.owlapi.apibinding.OWLManager



class DatabaseModelSpec extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter with DiagrammedAssertions {

  val prefix = "cancer#"

  val Thing = Class(s"http://www.w3.org/2002/07/owl#Thing")
  val diagnosedWith = ObjectProperty(s"${prefix}diagnosedWith")
  val someThing = ObjectProperty(s"${prefix}someThing")
  val dueTo = ObjectProperty(s"${prefix}dueTo")
  val causedBy = ObjectProperty(s"${prefix}causedBy")
  val SkinCancer = Class(s"${prefix}SkinCancer")
  val LungCancer = Class(s"${prefix}LungCancer")
  val Cancer = Class(s"${prefix}Cancer")
  val PancreasCancer = Class(s"${prefix}PancreasCancer")
  val SkinCancerPatient = Class(s"${prefix}SkinCancerPatient")
  val CancerPatient = Class(s"${prefix}CancerPatient")
  val BobsCancer = NamedIndividual(s"${prefix}BobsCancer")
  val Bob = NamedIndividual(s"${prefix}Bob")

  val test_ontology: String = """
    Prefix(:=<cancer#>)
  Prefix(owl:=<http://www.w3.org/2002/07/owl#>)
  Prefix(rdf:=<http://www.w3.org/1999/02/22-rdf-syntax-ns#>)
  Prefix(xml:=<http://www.w3.org/XML/1998/namespace>)
  Prefix(xsd:=<http://www.w3.org/2001/XMLSchema#>)
  Prefix(rdfs:=<http://www.w3.org/2000/01/rdf-schema#>)
  Prefix(time:=<http://www.w3.org/2006/time#>)

  Ontology(<cancer>
    Import(<http://www.w3.org/2006/time>)

      Declaration(Class(:Human))
      Declaration(Class(:Cancer))
      Declaration(Class(:CancerPatient))
      Declaration(Class(:PancreasCancer))
      Declaration(Class(:PancreasCancerPatient))
      Declaration(Class(:SkinCancer))
      Declaration(Class(:LungCancer))
      Declaration(Class(:SkinCancerPatient))
      Declaration(ObjectProperty(:causedBy))
      Declaration(ObjectProperty(:diagnosedWith))
      Declaration(ObjectProperty(:dueTo))
      Declaration(ObjectProperty(:someThing))
      Declaration(NamedIndividual(:Alice))
      Declaration(NamedIndividual(:Bob))
      Declaration(NamedIndividual(:Anvil))
      Declaration(NamedIndividual(:BobsCancer))
      Declaration(AnnotationProperty(:begin))
      Declaration(AnnotationProperty(:end))
      Declaration(AnnotationProperty(rdfs:datetime))
      Declaration(AnnotationProperty(:diamond))


      AnnotationAssertion(rdfs:label <:CancerPatient> "A patient that is diagnosed with Cancer"@en)

        ############################
        #   Object Properties
        ############################

        # Object Property: :causedBy (:causedBy)

        SubObjectPropertyOf(:causedBy :dueTo)
        SubObjectPropertyOf(:causedBy :someThing)

        # Object Property: :diagnosedWith (:diagnosedWith)

        SubObjectPropertyOf(:diagnosedWith :dueTo)

        # Object Property: :dueTo (:dueTo)

        SubObjectPropertyOf(:dueTo :causedBy)



        ############################
        #   Classes
        ############################

        # Class: :CancerPatient (:CancerPatient)

        EquivalentClasses(:CancerPatient ObjectSomeValuesFrom(:diagnosedWith :Cancer))
        EquivalentClasses(ObjectSomeValuesFrom(:diagnosedWith :PancreasCancer) :PancreasCancerPatient)
        EquivalentClasses(Annotation(:diamond "convex PT3S"^^xsd:string)ObjectSomeValuesFrom(:diagnosedWith :SkinCancer) :SkinCancerPatient)

        EquivalentClasses(Annotation(:diamond "convex PT3S"^^xsd:string) Annotation(:diamond "convex PT5S"^^xsd:string) :PancreasCancerPatient :PancreasCancerPatient)

        # Class: :PancreasCancer (:PancreasCancer)

        SubClassOf(:PancreasCancer :Cancer)

        SubClassOf(Annotation(:diamond "rigid"^^xsd:string) :PancreasCancer :PancreasCancer)
        SubClassOf(Annotation(:diamond "increasing"^^xsd:string) :PancreasCancer :PancreasCancer)
        SubClassOf(Annotation(:diamond "decreasing"^^xsd:string) :SkinCancer :Cancer)
        SubClassOf(Annotation(:diamond "convex"^^xsd:string) :Cancer :Cancer)


        # Class: :PancreasCancerPatient (:PancreasCancerPatient)



        # Class: :SkinCancer (:SkinCancer)

        SubClassOf(:SkinCancer :Cancer)

        SubClassOf(:LungCancer :Cancer)

        # Class: :SkinCancerPatient (:SkinCancerPatient)



        SubClassOf(:CancerPatient :Human)




        ############################
        #   Named Individuals
        ############################

        ClassAssertion(:Human :Anvil)
        ClassAssertion(:Human :Bob)
        ClassAssertion(:Human :Alice)

        # Individual: :Alice (:Alice)


        ClassAssertion(Annotation(time:instant "2011-10-02 18:48:05.123456"^^xsd:dateTime) :SkinCancerPatient :Alice)


        # Individual: :Bob (:Bob)

        ObjectPropertyAssertion(Annotation(time:instant"1994-10-02 18:48:05.123456"^^xsd:dateTime) :diagnosedWith :Bob :BobsCancer)
        ObjectPropertyAssertion(Annotation(time:instant"1994-10-10 18:48:05.123456"^^xsd:dateTime) :diagnosedWith :Bob :BobsCancer)

        # Individual: :BobsCancer (:BobsCancer)

        ClassAssertion(Annotation(time:instant "1994-10-02 18:48:05.123456"^^xsd:dateTime) :PancreasCancer :BobsCancer)
        ClassAssertion(Annotation(time:instant "1994-10-10 18:48:05.123456"^^xsd:dateTime) :PancreasCancer :BobsCancer)




        ClassAssertion(:CancerPatient :Anvil)
        ClassAssertion(:SkinCancerPatient :Anvil)


        )"""

  val dbparams = DBParams(s"jdbc:postgresql://localhost:5432", "testingcase_database_model_spec", "postgres", "")
  var dbman = DatabaseManager.getManager(dbparams)
  var dbmodel = DatabaseModel(dbman)


  before {
    dbman = DatabaseManager.getManager(dbparams)
    dbmodel = DatabaseModel(dbman)
  }

  override def afterAll() {
    dbman.close()
  }


  "Saving of an ontology" should "work" in {
    val manager = OWLManager.createOWLOntologyManager
    val stream = new ByteArrayInputStream(test_ontology.getBytes(StandardCharsets.UTF_8))
    val ont = manager.loadOntologyFromOntologyDocument(stream)
    val infOnt = OntologyUtils.classifyEL(ont)

    DatabaseManager.createDatabaseIfNotExists(dbparams)
    dbman = DatabaseManager.getManager(dbparams)
    DatabaseModel.saveToDatabase(infOnt, dbman)

  }

  "Queries" should "Class Hierarchy" in {
    var subClasses = dbmodel.querySubClasses(Cancer, false, true, true)
    assert(subClasses == Set(Cancer, SkinCancer, PancreasCancer, LungCancer))

    subClasses = dbmodel.querySubClasses(Cancer, true, true, true)
    assert(subClasses == Set(SkinCancer, PancreasCancer, LungCancer))

    subClasses = dbmodel.querySubClasses(Cancer, true, true, false)
    assert(subClasses == Set(SkinCancer, PancreasCancer, LungCancer))

    subClasses = dbmodel.querySubClasses(Cancer, true, false, false)
    assert(subClasses == Set(SkinCancer, PancreasCancer, LungCancer))

    subClasses = dbmodel.queryEquivalentClasses(Cancer)
    assert(subClasses == Set(Cancer))


    assert(dbmodel.querySuperClasses(SkinCancer, true, true, true) == Set(Cancer))
    assert(dbmodel.querySuperClasses(SkinCancer, false, true, false) == Set(SkinCancer, Cancer))
  }

  it should "SubObjectProperties" in {
    assert(dbmodel.queryEquivalentObjectProperties(diagnosedWith) == Set(diagnosedWith))
    assert(dbmodel.queryEquivalentObjectProperties(dueTo) == Set(dueTo, causedBy))
    assert(dbmodel.queryEquivalentObjectProperties(causedBy) == Set(dueTo, causedBy))

    assert(dbmodel.querySubObjectProperties(diagnosedWith, true) == Set())
    assert(dbmodel.querySubObjectProperties(diagnosedWith, false) == Set(diagnosedWith))
    assert(dbmodel.querySubObjectProperties(dueTo, true) == Set(diagnosedWith))
    assert(dbmodel.querySubObjectProperties(dueTo, false) == Set(diagnosedWith, dueTo, causedBy))

    assert(dbmodel.querySuperObjectProperties(diagnosedWith, false) == Set(diagnosedWith, dueTo, causedBy, someThing))


  }

  it should "InstanceQueries" in {
    assert(dbmodel.queryInstance(Cancer, BobsCancer))
    assert(!dbmodel.queryInstance(LungCancer, BobsCancer))
    assert(dbmodel.queryInstance(diagnosedWith, Bob, BobsCancer))
    assert(!dbmodel.queryInstance(diagnosedWith, BobsCancer, BobsCancer))

    assert(dbmodel.queryMembership(BobsCancer).toSet == Set(Cancer, PancreasCancer, Thing))


  }

  it should "Labels" in {
    assert(dbmodel.getLabels(CancerPatient) == Set("A patient that is diagnosed with Cancer"))
  }

  it should "Members" in {
    assert(dbmodel.queryMembers(Cancer).toSet == Set(BobsCancer))
    assert(dbmodel.queryMembers(diagnosedWith).toSet == Set((Bob, BobsCancer)))
  }





}