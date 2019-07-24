# EL2DB #

A simple scala library to map an ontology to a Postgresql database. 
This way the classification can be computed once and is saved to the DB. 
Supported operations are: 

`getLabels`
`getClasses`
`queryEquivalentClasses`
`querySubClasses`
`querySuperClasses`

`getObjectProperties`
`queryEquivalentObjectProperties`
`querySubObjectProperties`
`querySuperObjectProperties`


`queryInstance`
`queryMembers`

An easy way to use it in your project: 

# sbt publishLocal 

and the build.sbt of your project add the line 

# "de.tu-dresden" %% "el2db" % "0.1.0-SNAPSHOT"
