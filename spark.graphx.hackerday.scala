import org.apache.spark.graphx._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel.DISK_ONLY


case class Grid (id:Long, shape: String, numSides:Int, coordinates: Seq[Tuple2[Float, Float]], cellId: Long) {	
	override def toString = {
		s"Cell $cellId"
	}
}

def createGrids(row : Row) : (VertexId, Grid) = {
	val arrayofTypes = row.getAs[Seq[Seq[Float]]](3) map (i => (i(0), i(1))) 
	val id : VertexId = row.getLong(0)
	(id, Grid(row.getLong(0), row.getString(1), row.getInt(2), arrayofTypes, row.getLong(4)))
}

//read the json document into an RDD of Grids
val nodes = (sqlContext.read.json("/user/cloudera/rawdata/dandelion/milan/grid")
				.selectExpr("explode(features) as rec")
				.selectExpr("rec.id", "rec.geometry.type", "size(rec.geometry.coordinates[0]) as numSides", "rec.geometry.coordinates[0] as coordinates", "rec.properties.cellId")
			)  map (createGrids) // RDD(i, Grid())

val cdrRDD = sc.textFile("/user/cloudera/rawdata/dandelion/milan/mitomi")

val edges = (cdrRDD map ( cdr =>{
			val parts = cdr.split("\t")
			((parts(1).toInt, parts(2).toInt), (parts(0).toLong, parts(3).toFloat))
		}) 	
		// filter ((a : ((Int, Int),(Long, Float))) => a._1._1 <= 10 && (a._1._2 <= 10))  //comment for real run
		combineByKey(((a : (Long, Float)) =>  a._2), 
					((a : Float, b : (Long, Float)) =>  a + b._2), 
					((a : Float, b : Float) =>  a + b)
			) map ((l: ((Int, Int),Float)) => Edge[Float](l._1._1, l._1._2, l._2))
	) //((Int, Int), Float)

//create the graph object from nodes and edges
//also cache and return the newly created graph object
val graph = Graph(nodes, edges)

// number of edges
//graph.numEdges //22.5+ million edges

// number of vertices
//graph.numVertices //10001


def exportToGexf[VD, ED](g:Graph[VD, ED], fileName : String) = {
	// exporting the graph to Gephi's gexf file format
	def toGexf[VD,ED](g:Graph[VD,ED]) =
		("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
		"<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
		"	<graph mode=\"static\" defaultedgetype=\"directed\">\n" +
		"	<nodes>\n" +
					g.vertices.map(v => "			<node id=\"" + v._1 + "\" label=\"" +
					v._2 + "\" />\n").collect.mkString +
		"	</nodes>\n" +
		"	<edges>\n" +
					g.edges.map(e => "			<edge source=\"" + e.srcId +
						"\" target=\"" + e.dstId + "\" label=\"" + e.attr +
						"\" />\n").collect.mkString +
		"	</edges>\n" +
		" </graph>\n" +
		"</gexf>")
	///
	val pw = new java.io.PrintWriter(s"$fileName.gexf")
	pw.write(toGexf(g))
	pw.close
}

val subgraph = graph.subgraph(vpred = (id, _) => id <= 10)
exportToGexf(subgraph, "my_subgraph")