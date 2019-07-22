package de.tu_dresden

import java.sql.Timestamp

import com.typesafe.scalalogging.StrictLogging
import de.tu_dresden.el2db.RoleHierarchy
import org.semanticweb.elk.owlapi.ElkReasonerFactory
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model._
import org.semanticweb.owlapi.model.parameters.Imports
import org.semanticweb.owlapi.reasoner.InferenceType
import org.semanticweb.owlapi.util._
import uk.ac.manchester.cs.owl.owlapi.OWLAnnotationPropertyImpl
import org.phenoscape.scowl._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

package object OntologyUtils {
  implicit class OntologyTools(o: OWLOntology) extends StrictLogging{
    def getAnnotationAssertionAxioms(includeImportsClosure: Imports): List[OWLAnnotationAssertionAxiom] = {
      o.getABoxAxioms(includeImportsClosure).asScala.toList.filter(_.isInstanceOf[OWLAnnotationAssertionAxiom]).map(_.asInstanceOf[OWLAnnotationAssertionAxiom])
    }

    def getABoxAxioms(f: OWLAxiom => Boolean, includeImportsClosure: Imports): mutable.Set[OWLAxiom] = {
      o.getABoxAxioms(includeImportsClosure).asScala.filter(f)
    }

    def getABoxAxiomsAnnotatedByProperty(propertyType: IRI, includeImportsClosure: Imports): mutable.Set[OWLAxiom] = {
      val f = (a:OWLAxiom) => { !a.getAnnotations(new OWLAnnotationPropertyImpl(propertyType)).isEmpty }
      o.getABoxAxioms(f, includeImportsClosure)
    }

    def getABoxAxiomsNotAnnotatedByProperty(propertyType: IRI, includeImportsClosure: Imports): List[OWLAxiom] = {
      o.getABoxAxioms(includeImportsClosure).asScala.toList.filter{ a => a.getAnnotations(new OWLAnnotationPropertyImpl(propertyType)).isEmpty}
    }
  }

  implicit class IRI2SQL(o: HasIRI) {
    def toDB:String = o.getIRI.toString
    def toDBEsc:String = "'" + o.getIRI.toString + "'"
  }

  def option2DB(o:Option[Timestamp]) : String = {
    o match {
      case Some(t) => s"'${t.toString})}'"
      case None => "null"
    }
  }

  def classifyEL(ontology: OWLOntology): OWLOntology = {
    val outputOntologyManager = OWLManager.createOWLOntologyManager
    // Create an ELK reasoner.

    val reasonerFactory = new ElkReasonerFactory()
    val reasoner = reasonerFactory.createReasoner(ontology)

    // Classify the ontology.
    reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY)

    // To generate an inferred ontology we use implementations of
    // inferred axiom generators
      val gens = new ArrayBuffer[InferredAxiomGenerator[_ <: OWLAxiom]]
    gens.append(new InferredSubClassAxiomGenerator)
    gens.append(new InferredClassAssertionAxiomGenerator())

    // Put the inferred axioms into a fresh empty ontology.
    val infOnt = outputOntologyManager.createOntology(ontology.getAxioms(Imports.INCLUDED))
    val iog = new InferredOntologyGenerator(reasoner, gens.asJava)
    iog.fillOntology(outputOntologyManager.getOWLDataFactory, infOnt)

    // Add ObjectProperty Hierarchy

    val roleHierarchy = new RoleHierarchy(ontology)
    for (r <- roleHierarchy.roleHierarchy.keys) {
      val stmts = for (sc <- roleHierarchy.getSubRoles(r, true)) yield { SubObjectPropertyOf(sc, r) }
      outputOntologyManager.addAxioms(infOnt, stmts.asJava)
    }

    infOnt
  }
}
