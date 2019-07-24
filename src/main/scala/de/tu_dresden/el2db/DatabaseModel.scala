package de.tu_dresden.el2db

import java.sql.Timestamp

import com.typesafe.scalalogging.StrictLogging
import de.tu_dresden.OntologyUtils._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.parameters.Imports
import org.semanticweb.owlapi.model.{OWLOntology, _}
import org.semanticweb.owlapi.search.EntitySearcher
import slick.dbio.Effect.Write
import slick.jdbc.GetResult
import slick.jdbc.PostgresProfile.api._
import slick.lifted.{TableQuery, Tag}
import slick.sql.FixedSqlAction

import scala.collection.JavaConverters._
import scala.collection.immutable.Set
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DatabaseModel extends StrictLogging {
  def saveToDatabase(ontology: OWLOntology, manager: DatabaseManager) = {
    val el2db = DatabaseModel(manager)
    el2db.saveToDatabase(ontology)
    el2db
  }
}

case class DatabaseModel(manager: DatabaseManager) extends StrictLogging {


  private class HashCode(tag: Tag) extends Table[Int](tag, "hash_code") {
    def hash = column[Int]("hash_code", O.PrimaryKey)
    def * = (hash)
  }

  private class Concepts(tag: Tag) extends Table[String](tag, "concepts") {
    def iri = column[String]("iri", O.PrimaryKey)
    def * = (iri)
  }

  private class ConceptAnnotations(tag: Tag) extends Table[(String, String, Int)](tag, "concept_annotations") {
    def conceptiri = column[String]("concept_id")
    def label = column[String]("label")
    def ltype = column[Int]("type")
    def * = (conceptiri, label, ltype)
    def pk = index("pk_r", conceptiri)
    def concept = foreignKey("concept_fk", conceptiri, concepts)(_.iri)
  }

  private class Roles(tag: Tag) extends Table[(String)](tag, "roles") {
    def iri = column[String]("iri", O.PrimaryKey)
    def * = (iri)
  }

  private class RoleAnnotations(tag: Tag) extends Table[(String, String, Int)](tag, "role_annotations") {
    def roleiri = column[String]("role_id")
    def label = column[String]("label")
    def ltype = column[Int]("type")
    def * = (roleiri, label, ltype)
    def role = foreignKey("concept_fk", roleiri, roles)(_.iri)
  }

  private class Individuals(tag: Tag) extends Table[(String)](tag, "individuals") {
    def iri = column[String]("iri", O.PrimaryKey)
    def * = (iri)
  }

  private class IndividualAnnotations(tag: Tag) extends Table[(String, String, Int)](tag, "individual_annotations") {
    def indiri = column[String]("ind_id")
    def label = column[String]("label")
    def ltype = column[Int]("type")
    def * = (indiri, label, ltype)
    def role = foreignKey("ind_fk", indiri, individuals)(_.iri)
  }


  private class ConceptAssertions(tag: Tag) extends Table[(String, String, Option[Timestamp])](tag, "concept_assertions") {
    def conceptiri = column[String]("concept_id")
    def indid = column[String]("ind_id")
    def timestamp = column[Option[Timestamp]]("timestamp")

    def * = (conceptiri, indid, timestamp)

    def pk = index("pk_c", (conceptiri, indid, timestamp), unique = true)

    def idx = index("idx_c", (conceptiri), unique = false)


    def idx1 = index("idx_indid_name", (indid), unique = false)

    // A reified foreign key relation that can be navigated to create a join
    def concept = foreignKey("concept_fk", conceptiri, concepts)(_.iri)

    def individual = foreignKey("individual_fk", indid, individuals)(_.iri)
  }

  private class RoleAssertions(tag: Tag) extends Table[(String, String, String, Option[Timestamp])](tag, "role_assertions") {
    def roleiri = column[String]("role_id")

    def ind1id = column[String]("ind_1_id")

    def ind2id = column[String]("ind_2_id")

    def timestamp = column[Option[Timestamp]]("timestamp")

    def * = (roleiri, ind1id, ind2id, timestamp)

    def pk = index("pk_ra", (roleiri, ind1id, ind2id, timestamp), unique = true)

    def idx = index("idx_ra", (roleiri), unique = false)


    def idx1 = index("idx_indids_name", (ind1id, ind2id), unique = false)

    def role = foreignKey("role_fk", roleiri, roles)(_.iri)

    def individual1 = foreignKey("individual_fk_1", ind1id, individuals)(_.iri)

    def individual2 = foreignKey("individual_fk_2", ind2id, individuals)(_.iri)
  }

  private class SubObjectProperties(tag: Tag) extends Table[(String, String)](tag, "sub_roles") {
    def roleiri = column[String]("role_id")

    def subroleiri = column[String]("sub_role_id")

    def * = (roleiri, subroleiri)

    def pk = primaryKey("pk_sr", (roleiri, subroleiri))

    def idx1 = index("idx_r1", (roleiri), unique = false)

    def idx2 = index("idx_r2", (subroleiri), unique = false)

    def role = foreignKey("role_fk", roleiri, roles)(_.iri)

    def subrole = foreignKey("subrole_fk", subroleiri, roles)(_.iri)
  }

  private class SubConcepts(tag: Tag) extends Table[(String, String)](tag, "sub_concepts") {
    def conceptiri = column[String]("concept_id")

    def subconceptiri = column[String]("sub_concept_id")

    def * = (conceptiri, subconceptiri)

    def pk = primaryKey("pk_sc", (conceptiri, subconceptiri))

    def idx1 = index("idx_c1", (conceptiri), unique = false)

    def idx2 = index("idx_c2", (conceptiri), unique = false)

    def concept = foreignKey("concept_fk", conceptiri, concepts)(_.iri)

    def subconcept = foreignKey("subconcept_fk", subconceptiri, concepts)(_.iri)
  }

  private val concepts = TableQuery[Concepts]
  private val classAnnotations = TableQuery[ConceptAnnotations]
  private val roles = TableQuery[Roles]
  private val objectPropertyAnnotations = TableQuery[RoleAnnotations]
  private val individuals = TableQuery[Individuals]
  private val individualAnnotations = TableQuery[IndividualAnnotations]
  private val classAssertions = TableQuery[ConceptAssertions]
  private val roleAssertions = TableQuery[RoleAssertions]
  private val subConcepts = TableQuery[SubConcepts]
  private val subObjectProperties = TableQuery[SubObjectProperties]
  private val ontologyhashCode = TableQuery[HashCode]


  private val allRelations = List(
    individuals.schema,
    individualAnnotations.schema,
    roles.schema,
    objectPropertyAnnotations.schema,
    concepts.schema,
    classAnnotations.schema,
    classAssertions.schema,
    roleAssertions.schema,
    subConcepts.schema,
    subObjectProperties.schema,
    ontologyhashCode.schema)


  def truncate = {
    logger.debug("Truncating the tables if existing")
    try {
      val truncateStmt =
        """
            TRUNCATE TABLE individuals, concepts, roles, hash_code CASCADE;
          """.stripMargin
      manager.raw(truncateStmt)
    }
    catch {
      case e: org.postgresql.util.PSQLException => {
        //logger.debug(s"PSQL Error ${e.getSQLState}")
        e.getSQLState match {
          case "42P01" => {
            logger.info("Table don't exist")
          }
          case _ => throw e
        }
      }
    }
  }

  private def setup(resetSchema: Boolean = false) = {
    resetSchema match {
      case false => {
        logger.debug("Truncating the tables if existing")
        try {
          val truncateStmt =
            """
            TRUNCATE TABLE individuals, concepts, roles, hash_code CASCADE;
          """.stripMargin
          manager.raw(truncateStmt)
        }
        catch {
          case e: org.postgresql.util.PSQLException => {
            //logger.debug(s"PSQL Error ${e.getSQLState}")
            e.getSQLState match {
              case "42P01" => {
                logger.info("Table don't exist. Creating them..")
                manager.withDatabase(_.run(allRelations.reduce(_ ++ _).create))
                classAssertions.schema.createStatements.foreach(println)
              }
              case _ => throw e
            }
          }
        }
      }
      case true => {
        logger.debug("Dropping the schema")
        val resetStmt =
          """DROP SCHEMA public CASCADE;
                      CREATE SCHEMA public;
                    GRANT ALL ON SCHEMA public TO postgres;
                      GRANT ALL ON SCHEMA public TO public;"""
        manager.raw(resetStmt)
        logger.debug("Creating the tables")
        manager.withDatabase(_.run(allRelations.reduce(_ ++ _).create))
      }
    }


    //println(allRelations.reverse.reduce(_ ++ _).drop.statements.mkString("\n"))
    //manager.withDatabase( _.run(allRelations.reverse.reduce(_ ++ _).drop))

  }



  def updateHash(ontology: OWLOntology): Unit = {
      updateHash(ontology.hashCode())
  }

  def updateHash(hash: Int): Unit = {
    manager.run(ontologyhashCode.delete)
    manager.run(ontologyhashCode += hash)
  }

  /**
    * Append the signature of a given ontology to the database
    *
    * @param ontology
    */
  private def appendSignatureToDatabase(ontology: OWLOntology): Unit = {
    logger.debug("Saving Individuals Signature")
    val inds = ontology.getIndividualsInSignature(Imports.INCLUDED).asScala.toSeq.map(_.toDB).toSet
    manager.run(DBIO.seq(individuals ++= inds))

    logger.debug("Saving Concepts Signature")
    val cls = ontology.getClassesInSignature(Imports.INCLUDED).asScala.toSeq.map(_.toDB).toSet
    manager.run(DBIO.seq(concepts ++= cls))

    logger.debug("Saving Roles Signature")
    val rls = ontology.getObjectPropertiesInSignature(Imports.INCLUDED).asScala.toSeq.map(_.toDB).toSet
    manager.run(DBIO.seq(roles ++= rls))
  }

  private def writeToDBInChunks[A, B <: Table[_]](elems: Iterable[A], table: TableQuery[B], chunk_size: Int = 100)(f: A => Option[_ <: B#TableElementType]) = {
    logger.debug(s"Writing to DB in chunks of $chunk_size")
    var stmts = mutable.MutableList.empty
    var i = 1
    elems.foreach { el =>
      f(el) match {
        case Some(v) => {
          i += 1
          if (i % chunk_size == 0) {
            logger.debug(s"$i elements processed, saving to DB")
            manager.run(DBIO.seq(table ++= stmts))
            stmts.clear()
          }
        }
        case None =>
      }
    }
    manager.run(DBIO.seq(table ++= stmts))
    stmts.clear()
    logger.debug(s"Saved $i elements to DB.")
  }

  private def appendAnnotationsToDatabase(ontology: OWLOntology) = {
    var stmts: mutable.MutableList[(String, String, Int)] = mutable.MutableList.empty

    var list = List((ontology.getClassesInSignature(Imports.INCLUDED).asScala,classAnnotations),
      (ontology.getObjectPropertiesInSignature(Imports.INCLUDED).asScala,objectPropertyAnnotations),
      (ontology.getIndividualsInSignature(Imports.INCLUDED).asScala,individualAnnotations))

    var i = 1

    val labelProperties = Set("http://snomed.info/field/Description.term.en-us.preferred", "http://snomed.info/field/Description.term.en-us.synonym", "http://www.w3.org/2000/01/rdf-schema#label").map(IRI.create(_))
    def isLabel(annotation: OWLAnnotation): Boolean = {
      labelProperties.contains(annotation.getProperty.getIRI)
    }

    for ( (s,t) <- list) yield {
      for {e <- s;
           a <- EntitySearcher.getAnnotations(e, ontology).asScala;
           if isLabel(a) && a.getValue.isInstanceOf[OWLLiteral]} yield {
        val value = a.getValue
        stmts.+:=(e.toDB, value.asInstanceOf[OWLLiteral].getLiteral, 0)

        i += 1
        if (i % 1000 == 0) {
          logger.debug(s"$i elements processed, saving to DB")
          manager.run(DBIO.seq(t ++= stmts))
          stmts.clear()
        }
      }

      logger.debug(s"$i elements processed, saving to DB")
      manager.run(DBIO.seq(t ++= stmts))
      stmts.clear()
    }
  }

  private def appendHierarchyToDatabase(ontology: OWLOntology): Unit = {
    var stmts:ListBuffer[FixedSqlAction[Any, NoStream, Write]] = ListBuffer[FixedSqlAction[Any, NoStream, Write]]()
    for (ax <- ontology.getTBoxAxioms(Imports.INCLUDED).asScala.toSeq) yield {
      ax match {
        case SubClassOf(_, c1@Class(_), c2@Class(_)) if c1 != c2 => stmts.append(subConcepts += (c2.toDB, c1.toDB))
        case a@EquivalentClasses(_, cs) => {
          for ( as <- a.asPairwiseAxioms().asScala;
                SubClassOf(_, c1@Class(_), c2@Class(_)) <- as.asOWLSubClassOfAxioms().asScala;
                if c1 != c2
                ) yield {
            stmts.append(subConcepts += (c2.toDB, c1.toDB))
          }
        }
        case _ =>
      }
    }

    manager.run(DBIO.seq(stmts.map(_.asTry): _*))

    for (ax <- ontology.getRBoxAxioms(Imports.INCLUDED).asScala.toSeq) yield {
      ax match {
        case SubObjectPropertyOf(_, r1, r2) if r1 != r2 =>
          stmts.append(subObjectProperties += (r2.asOWLObjectProperty().toDB, r1.asOWLObjectProperty().toDB))
        case a@EquivalentObjectProperties(_, rs) => {
          for ( as <- a.asPairwiseAxioms().asScala;
                SubClassOf(_, r1@ObjectProperty(_), r2@ObjectProperty(_)) <- as.asSubObjectPropertyOfAxioms().asScala;
                if r1 != r2
          ) yield {
            stmts.append(subObjectProperties += (r2.toDB, r1.toDB))
          }
        }
        case _ =>
      }
    }

    manager.run(DBIO.seq(stmts.map(_.asTry): _*))

    logger.debug(s"Wrote hierarchy.")

  }



  private def appendAssertionsToDatabase(ontology: OWLOntology): Unit = {
    // Load Reasoner and do the inference
    //val reasoner = new ElkReasonerFactory().createReasoner(ontology, new SimpleConfiguration())
    //reasoner.precomputeInferences(InferenceType.CLASS_ASSERTIONS, InferenceType.DATA_PROPERTY_ASSERTIONS)

    val cls = ontology.getClassesInSignature(Imports.INCLUDED)
    var i = 1
    var stmts: mutable.MutableList[(String, String)] = mutable.MutableList.empty
    cls.forEach { case cls: OWLClass =>
      i += 1;

      stmts.++=(ontology.getClassAssertionAxioms(cls).asScala.toSeq.map(i => (cls.toDBEsc, i.getIndividual.asOWLNamedIndividual().toDBEsc)))
      if (i % 1000 == 0) {
        logger.debug(s"$i concept assertions processed. Saving to DB.")
        manager.insert("concept_assertions", stmts.map(_.toString))
        //manager.run(DBIO.seq(conceptAssertions ++= stmts))
        stmts.clear
      }
    }
    logger.debug(s"$i concept assertions processed. Saving to DB.")
    manager.insert("concept_assertions", stmts.map(_.toString))
    //manager.run(DBIO.seq(conceptAssertions ++= stmts))
    stmts.clear

    logger.debug("Adding role assertions")
    // Construct Role Assertions

    //val roleHierarchy = new RoleHierarchy(ontology)
    i = 1
    var stmtsR: mutable.MutableList[(String, String, String)] = mutable.MutableList.empty
    for {ax <- ontology.getABoxAxioms(Imports.INCLUDED).asScala;
         if ax.isInstanceOf[OWLObjectPropertyAssertionAxiom]} {
      val ax1: OWLObjectPropertyAssertionAxiom = ax.asInstanceOf[OWLObjectPropertyAssertionAxiom]
      val role = ax1.getProperty.asOWLObjectProperty()
      val ind1 = ax1.getSubject.asOWLNamedIndividual()
      val ind2 = ax1.getObject.asOWLNamedIndividual()
      val x = ax1.getAnnotations() //(new OWLAnnotationPropertyImpl(IRI.create(":timestamp"))).asScala
      for (t <- x.asScala) {
        logger.debug(t.toString)
      }
      stmtsR.++=(for (superRole <- Seq(role)) yield {
        (superRole.toDBEsc, ind1.toDBEsc, ind2.toDBEsc)
      })
      if (i % 1000 == 0) {
        logger.debug(s"$i role assertions processed. Saving to DB.")
        manager.insert("role_assertions", stmtsR.map(_.toString))
        //manager.run(DBIO.seq(roleAssertions ++= stmtsR))
        stmtsR.clear
      }
    }
    logger.debug(s"$i role assertions processed. Saving to DB.")
    manager.insert("role_assertions", stmtsR.map(_.toString))
    //manager.run(DBIO.seq(roleAssertions ++= stmtsR))
    stmtsR.clear
  }



  private def setupToDatabase(ontology: OWLOntology) = {
    logger.info("Setting up database schema")
    setup(false)
    logger.info("Saving signature to database")
    appendSignatureToDatabase(ontology)

  }

  def saveAnnotations(ontology: OWLOntology) = {
    logger.info("Saving annotations to database")
    manager.run(individualAnnotations.schema.truncate)
    manager.run(classAnnotations.schema.truncate)
    manager.run(objectPropertyAnnotations.schema.truncate)
    appendAnnotationsToDatabase(ontology)
  }

  private def saveHierarchy(ontology: OWLOntology) = {
    logger.info("Saving hierarchy to database")
    manager.run(subConcepts.schema.truncate)
    manager.run(subObjectProperties.schema.truncate)
    appendHierarchyToDatabase(ontology)

  }

  /*private def appendHierarchyToDatabaseFast(ontology: OWLOntology) = {

    // To generate an inferred ontology we use implementations of
    // inferred axiom generators

    // Put the inferred axioms into a fresh empty ontology.
    val gens: collection.mutable.Buffer[InferredAxiomGenerator[_ <: OWLAxiom]] = collection.mutable.Buffer(new InferredSubClassAxiomGenerator)
    val iog = new InferredOntologyGenerator(helper.reasoner, gens.asJava)

    val outputOntologyManager = OWLManager.createOWLOntologyManager
    val infOnt = outputOntologyManager.createOntology
    iog.fillOntology(outputOntologyManager.getOWLDataFactory, infOnt)


    logger.debug("Creating concepts hierarchy")
    val axioms = infOnt.getAxioms(AxiomType.SUBCLASS_OF).asScala

    def f(ax: OWLSubClassOfAxiom): Option[(String, String)] = {
      if (ax.getSubClass.isInstanceOf[OWLClass] && ax.getSuperClass.isInstanceOf[OWLClass]) {
        val sup = ax.getSuperClass.asOWLClass()
        val sub = ax.getSubClass.asOWLClass()
        if (sup.isOWLThing) None
        else Some((sup.toDB, sub.toDB))
      } else None
    }

    logger.debug(s"Writing to DB in chunks of 1000")
    var stmts: mutable.MutableList[(String, String)] = mutable.MutableList.empty
    var i = 1
    axioms.foreach { el =>
      f(el) match {
        case Some(v) => {
          i += 1
          stmts.+:=(v)
          if (i % 1000 == 0) {
            logger.debug(s"$i elements processed, saving to DB")
            manager.run(DBIO.seq(subConcepts ++= stmts))
            stmts.clear()
          }
        }
        case None =>
      }
    }

    manager.run(DBIO.seq(subConcepts ++= stmts))
    stmts.clear()

    logger.debug(s"Saved $i elements to DB.")

    //writeToDBInChunks(axioms, subConcepts, 1000)(f)
    logger.debug(s"Added subclass assertions to the hierarchy.")

    logger.debug("Creating roles hierarchy")
    val rolesList: Seq[(String)] = manager.withDatabase(db => db.run((for {c <- roles} yield (c.iri)).result))
    for ((iri) <- rolesList) {
      val c = ObjectProperty(iri)
      val stmts = for (sc <- helper.querySubRoles(c, false).toSeq.map(_.toDB);
                       if iri != sc) yield {
        subRoles += (iri, sc)
      }
      manager.run(DBIO.seq(stmts: _*))
    }
  }
  */

  private def saveAssertions(ontology: OWLOntology) = {
    logger.info("Saving assertions to database")
    manager.run(classAssertions.schema.truncate)
    manager.run(roleAssertions.schema.truncate)
    appendAssertionsToDatabase(ontology)
  }

  def saveToDatabase(ontology: OWLOntology): Unit = {
    setupToDatabase(ontology)
    saveAnnotations(ontology)
    saveAssertions(ontology)
    saveHierarchy(ontology)
    updateHash(ontology)
  }


  def pShow: String = throw new NotImplementedError("Please implement pShow")

  private def s2Ind(s: String): OWLNamedIndividual = NamedIndividual(s)

  def queryMembers(cls: OWLClass): Traversable[OWLNamedIndividual] = {
    val q = for {
      c <- concepts; if c.iri === cls.toDB
      a <- classAssertions; if a.conceptiri === c.iri
      i <- individuals; if i.iri === a.indid
    } yield i.iri
    val r: Seq[String] = manager.withDatabase(db => db.run(q.result))
    r.map(s2Ind)
  }

  def queryMembers(role: OWLObjectProperty): Traversable[(OWLNamedIndividual, OWLNamedIndividual)] = {
    val q = for {
      r <- roles; if r.iri === role.toDB
      a <- roleAssertions; if a.roleiri === r.iri
      i1 <- individuals; if i1.iri === a.ind1id
      i2 <- individuals; if i2.iri === a.ind2id
    } yield (i1.iri, i2.iri)
    val r: Seq[(String, String)] = manager.withDatabase(db => db.run(q.result))
    r.map { case (s1, s2) => (s2Ind(s1), s2Ind(s2)) }
  }

  def getIndividuals: Traversable[OWLNamedIndividual] = {
    val q = for {
      i <- individuals
    } yield i.iri
    val r: Seq[String] = manager.withDatabase(db => db.run(q.result))
    r.map(s2Ind)
  }

  def getClasses: Traversable[OWLClass] = {
    val q = for {
      i <- concepts
    } yield i.iri
    val r: Seq[String] = manager.withDatabase(db => db.run(q.result))
    r.map(s => Class(s))
  }


  def getObjectProperties: Traversable[OWLObjectProperty] = {
    val q = for {
      i <- roles
    } yield i.iri
    val r: Seq[String] = manager.withDatabase(db => db.run(q.result))
    r.map(s => ObjectProperty(s))
  }


  def queryMembership(ind: OWLNamedIndividual): Iterable[OWLClass] = {
    val q = for {
      a <- classAssertions; if a.indid === ind.toDB
    } yield a.conceptiri
    val r: Seq[String] = manager.withDatabase(db => db.run(q.distinct.result))
    r.map(s => Class(s))
  }

  def queryInstance(cls: OWLClass, term: OWLNamedIndividual): Boolean = {
    val q = for {
      a <- classAssertions; if a.indid === term.toDB && a.conceptiri === cls.toDB
    } yield a.indid
    manager.withDatabase(db => db.run(q.exists.result))
  }

  def queryInstance(role: OWLObjectProperty, term1: OWLNamedIndividual, term2: OWLNamedIndividual): Boolean = {
    val q = for {
      a <- roleAssertions; if a.ind1id === term1.toDB && a.ind2id === term2.toDB && a.roleiri === role.toDB
    } yield a.roleiri
    manager.withDatabase(db => db.run(q.exists.result))
  }

  def getLabels(cls: OWLClass): Seq[String] = {
    val q = for {
      l <- classAnnotations; if l.conceptiri === cls.toDB
    } yield l.label
    manager.withDatabase(db => db.run(q.result))
  }

  def getLabels(role: OWLObjectProperty): Seq[String] = {
    val q = for {
      l <- objectPropertyAnnotations; if l.roleiri === role.toDB
    } yield l.label
    manager.withDatabase(db => db.run(q.result))
  }

  def getLabels(ind: OWLNamedIndividual): Seq[String] = {
    val q = for {
      l <- individualAnnotations; if l.indiri === ind.toDB
    } yield l.label
    manager.withDatabase(db => db.run(q.result))
  }


  def getSignatureCounts: (Int, Int, Int) = {
    val i: Int = manager.withDatabase(_.run(individuals.length.result))
    val c: Int = manager.withDatabase(_.run(concepts.length.result))
    val r: Int = manager.withDatabase(_.run(roles.length.result))
    (i, c, r)
  }

  def isInitialized(ontology: OWLOntology): Boolean = {
    /*try {
      val hash:Int = manager.withDatabase(_.run(hashCode.result))
      ontology.hashCode() == hash
      /*hash match {
        case Some(h) => ontology.hashCode() == h
        case None => false
      }*/
    } catch {
      case e: org.postgresql.util.PSQLException => {
        logger.debug("Database does not exist; Model is not initialized")
        e.getSQLState match {
          case "42P01" => return false
          case _ => throw e
        }
      }
    }
    */

    try {
      logger.debug(s"Checking if model is initalized for db ${manager.dbname}..")
      logger.debug(manager.dbparams.toString)
      val i = ontology.getIndividualsInSignature(Imports.INCLUDED).size()
      val c = ontology.getClassesInSignature(Imports.INCLUDED).size()
      val r = ontology.getObjectPropertiesInSignature(Imports.INCLUDED).size()
      val ocnts = (i, c, r)
      val cnts = getSignatureCounts
      logger.info(s"Comparing signature dbcounts == ontologycounts; $cnts == $ocnts")
      cnts == ocnts
    } catch {
      case e: org.postgresql.util.PSQLException => {
        logger.debug("Database does not exist; Model is not initialized")
        e.getSQLState match {
          case "42P01" => return false
          case _ => throw e
        }
      }
    }

  }

  implicit private val getListStringResult = GetResult[List[String]](
    prs => (1 to prs.numColumns).map(_ => prs.nextString).toList
  )



  private def subClassQuery(cls: Rep[String]) = for
    (c <- subConcepts; if c.conceptiri === cls)
    yield (c.subconceptiri)

  private val subclassQueryCompiled = Compiled(subClassQuery _)

  def querySubClasses(query: OWLClass, strict: Boolean, omitTop: Boolean, direct: Boolean): Set[OWLClass] = {
    val clsT:Set[String] = direct match {
      case true => {
        manager.run(subclassQueryCompiled(query.toDB).result).toSet
      }
      case false => {
        querySubClassesRecursive(query.toDB)
      }
    }

    var cls = clsT.map(Class(_))
    if (omitTop) cls = cls.filter(!_.isOWLThing)
    if (strict) cls = cls.diff(queryEquivalentClasses(query))
    else cls = cls.+(query)
    return cls
  }



  private def querySubClassesRecursive(cls: String): Set[String] = {
    val r: Set[String] = manager.run(subclassQueryCompiled(cls).result).toSet
    r++(r.flatMap(querySubClassesRecursive(_)))
  }

  def queryEquivalentClasses(cls: OWLClass): Set[OWLClass] = {
    val q = for
      { c <- subConcepts if c.conceptiri === cls.toDB
        sr <- subConcepts if sr.conceptiri === c.subconceptiri && sr.subconceptiri === c.conceptiri }
      yield (c.subconceptiri)
    manager.run(q.result).map(Class(_)).toSet.+(cls)

    //val subCls = querySubClasses(cls, false, false, true)
    //subCls.filter(a => querySubClasses(a, false, false, true).contains(cls))
  }

  private def superClassQuery(cls: Rep[String]) = for
    (c <- subConcepts; if c.subconceptiri === cls)
    yield (c.conceptiri)

  private val superclassQueryCompiled = Compiled(superClassQuery _)

  private def querySuperClassesRecursive(cls: String): Set[String] = {
    val r: Set[String] = manager.run(superclassQueryCompiled(cls).result).toSet
    r++(r.flatMap(querySuperClassesRecursive(_)))
  }


  def querySuperClasses(query: OWLClass, strict: Boolean, omitTop: Boolean, direct: Boolean): Set[OWLClass] = {
    val clsT:Set[String] = direct match {
      case true => {
        manager.run(superclassQueryCompiled(query.toDB).result).toSet
      }
      case false => {
        querySuperClassesRecursive(query.toDB)
      }
    }

    var cls = clsT.map(Class(_))
    if (omitTop) cls = cls.filter(!_.isOWLThing)
    if (strict) cls = cls.diff(queryEquivalentClasses(query))
    else cls = cls.+(query)
    return cls
  }

  private def subRolesQuery(role: Rep[String]) = for
    (c <- subObjectProperties; if c.roleiri === role)
    yield (c.subroleiri)

  private val subRolesQueryCompiled = Compiled(subRolesQuery _)

  def querySubObjectProperties(query: OWLObjectProperty, strict: Boolean): Set[OWLObjectProperty] = {
    val clsT:Set[String] = querySubObjectPropertiesRecursive(List(query.toDB))

    var cls = clsT.map(ObjectProperty(_))
    if (strict) cls = cls.diff(queryEquivalentObjectProperties(query))
    else cls = cls.+(query)
    return cls
  }

  private def querySubObjectPropertiesRecursive(queue: List[String], processed: Set[String] = Set()): Set[String] = {
    if (queue.isEmpty) processed
    else {
      val e = queue.head
      if (processed.contains(e)) querySubObjectPropertiesRecursive(queue.tail, processed)
      else {
        val newRoles = manager.run(subRolesQueryCompiled(queue.head).result).toSet
        val newQueue = queue.tail ++ newRoles
        val newProcessed = processed + e
        querySubObjectPropertiesRecursive(newQueue, newProcessed)
      }
    }
  }

  private def superRolesQuery(role: Rep[String]) = for
    (c <- subObjectProperties; if c.subroleiri === role)
    yield (c.roleiri)

  private val superRolesQueryCompiled = Compiled(superRolesQuery _)

  def querySuperObjectProperties(query: OWLObjectProperty, strict: Boolean): Set[OWLObjectProperty] = {
    val clsT:Set[String] = querySuperObjectPropertiesRecursive(List(query.toDB))

    var cls = clsT.map(ObjectProperty(_))
    if (strict) cls = cls.diff(queryEquivalentObjectProperties(query))
    else cls = cls.+(query)
    return cls
  }

  private def querySuperObjectPropertiesRecursive(queue: List[String], processed: Set[String] = Set()): Set[String] = {
    if (queue.isEmpty) processed
    else {
      val e = queue.head
      if (processed.contains(e)) querySuperObjectPropertiesRecursive(queue.tail, processed)
      else {
        val newRoles = manager.run(superRolesQueryCompiled(queue.head).result).toSet
        val newQueue = queue.tail ++ newRoles
        val newProcessed = processed + e
        querySuperObjectPropertiesRecursive(newQueue, newProcessed)
      }
    }
  }


  def queryEquivalentObjectProperties(query: OWLObjectProperty): Set[OWLObjectProperty] = {
    val q = for
      {c <- subObjectProperties if c.roleiri === query.toDB
       sr <- subObjectProperties if sr.roleiri === c.subroleiri && sr.subroleiri === c.roleiri }
      yield (c.subroleiri)
    manager.run(q.result).map(ObjectProperty(_)).toSet + query
    //val subRoles = querySubRoles(query, false)
    //subRoles.filter(a => querySubRoles(a, false).contains(query))
  }
}
