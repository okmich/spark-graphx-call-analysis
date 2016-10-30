// cellid	outdegre	indegree	sumIndegVal, measures..	


// -- connected components
// -- extract batch i from the a certain part of the connected compoent result
// -- subtract batch i graph from the whole graph as new graph
// -- repeat until complete batch



// -- get the cog (using page rank) for the entire graph interaction
// -- get the connected component to the cog (the grid with the most inDeg and outDeg with the cog) as batch i
// -- subtract batch i graph from the whole graph as new whole graph
// -- repeat until complete batch


val inDegVert = graph.inDegrees
//see some samples 
inDegVert.sortBy(_._2, false).take(10).foreach(println)
inDegVert.sortBy(_._2, true).take(10).foreach(println)

val outDegVert = graph.outDegrees
//see some samples 
outDegVert.sortBy(_._2, false).take(10).foreach(println)
outDegVert.sortBy(_._2, true).take(10).foreach(println)

//create a data structure of cellId, outdegree, indegree
val tab1 = outDegVert.leftOuterJoin(inDegVert) map (m => (m._1, (m._2._1, m._2._2 match {
		case Some(x) => x
		case None => 0f
	})))

val inDegVal = graph.aggregateMessages[Float](ctx => ctx.sendToDst(ctx.attr), _ + _)
inDegVal.sortBy(_._2, false).take(10).foreach(println)
inDegVal.sortBy(_._2, true).take(10).foreach(println)

val outDegVal = graph.aggregateMessages[Float](ctx => ctx.sendToSrc(ctx.attr), _ + _)
outDegVal.sortBy(_._2, false).take(10).foreach(println)
outDegVal.sortBy(_._2, true).take(10).foreach(println)

//create a data structure of cellId, outdegree, indegree
val tab2 = outDegVal.leftOuterJoin(inDegVal) map (m => (m._1, (m._2._1, m._2._2 match {
		case Some(x) => x
		case None => 0f
	})))

//create a new structure with cellId, outdeg, indeg, outdegval, indegval
val tab12 = tab1.join(tab2) map (i => (i._1, (i._2._1._1, i._2._1._2, i._2._2._1,i._2._2._2)))



//identify within-cell call iteraction graph
val withinCellSubGraph = graph.subgraph(epred = et => et.srcId == et.dstId) //
val winthinCelCallStrength = withinCellSubGraph.aggregateMessages[Float](ctx => ctx.sendToSrc(ctx.attr), _ + _)
winthinCelCallStrength.sortBy(_._2, false).take(10).foreach(println)
winthinCelCallStrength.sortBy(_._2, true).take(10).foreach(println)

//cellId, outdeg, indeg, outdegval, indegval, withinGridVal
val tab22 = tab12.leftOuterJoin(winthinCelCallStrength) map (i => {
		val strg : Float = i._2._2 match {
			case Some(x) => x
			case None => 0f
		}
		(i._1, (i._2._1._1, i._2._1._2, i._2._1._3, i._2._1._4, strg))
	}) map (i => s"${i._1}\t${i._2._1}\t${i._2._2}\t${i._2._3}\t${i._2._4}\t${i._2._5}")

//save to hdfs
tab22.saveAsTextFile("/user/cloudera/output/dandelion/milan/interaction_output")


//you can optionall use a hive create table to make the dataset available for the ds to query from
