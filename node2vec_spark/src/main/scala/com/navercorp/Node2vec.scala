package com.navercorp


import java.io.Serializable

import scala.util.Try
import scala.collection.mutable.ArrayBuffer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{EdgeTriplet, Graph, _}
import com.navercorp.graph.{EdgeAttr, GraphOps, NodeAttr}
import org.apache.spark.broadcast.Broadcast

object Node2vec extends Serializable {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName);
  
  var context: SparkContext = _
  var config: Main.Params = _
  var node2id: RDD[(String, Long)] = _
  var indexedEdges: RDD[Edge[EdgeAttr]] = _
  var indexedNodes: RDD[(VertexId, NodeAttr)] = _
  var graph: Graph[NodeAttr, EdgeAttr] = _
  var randomWalkPaths: RDD[(Long, ArrayBuffer[Long])] = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param

    Util.deleteFile(config.output)(context, logger)
    this
  }
  
  /**
   * convertToNode2VecGraph
   * @return
   */
  def load(): this.type = {
    val bcMaxDegree = context.broadcast(config.degree)
    // 广播函数的作用是每个executor只有一个函数实例，节省“task执行自己编写的代码”的内存么
    val bcEdgeCreator: Broadcast[(VertexId, VertexId, Double) => Array[(VertexId, Array[(VertexId, Double)])]] = config.directed match {
      case true => context.broadcast(GraphOps.createDirectedEdge)
      case false => context.broadcast(GraphOps.createUndirectedEdge)
    }
    
    val inputTriplets: RDD[(Long, Long, Double)] = config.indexed match {
      case true => readIndexedGraph(config.input)
      case false => indexingGraph(config.input)
    }
    
    indexedNodes = inputTriplets.flatMap { case (srcId, dstId, weight) =>
      bcEdgeCreator.value.apply(srcId, dstId, weight)
    }.reduceByKey(_++_).map { case (dstNodeId, neighbors: Array[(VertexId, Double)]) =>
      var neighbors_ = neighbors
      if (neighbors_.length > bcMaxDegree.value) {
        neighbors_ = neighbors.sortWith{ case (left, right) => left._2 > right._2 }.slice(0, bcMaxDegree.value)
      }
        
      (dstNodeId, NodeAttr(neighbors = neighbors_.distinct))
    }.repartition(200).cache
    
    indexedEdges = indexedNodes.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
          Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(200).cache
    
    this
  }
  
  def initTransitionProb(): this.type = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    
    graph = Graph(indexedNodes, indexedEdges)
            .mapVertices[NodeAttr] { case (vertexId, clickNode) =>
              if (null != clickNode) { // 加null判断的原因： https://github.com/aditya-grover/node2vec/issues/29
                val (j, q) = GraphOps.setupAlias(clickNode.neighbors)
                val nextNodeIndex: PartitionID = GraphOps.drawAlias(j, q)
                clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1) // 这时候path有两个元素了： vertexId和根据weight选择的某一邻居节点
                
                clickNode
              } else {
                NodeAttr()
              }
            }
            .mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
              val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, edgeTriplet.srcAttr.neighbors, edgeTriplet.dstAttr.neighbors)
              edgeTriplet.attr.J = j
              edgeTriplet.attr.q = q
              edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)
              
              edgeTriplet.attr
            }.cache
    
    this
  }
  
  def randomWalk(): this.type = {
    val edge2attr: RDD[(String, EdgeAttr)] = graph.triplets.map { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
      (s"${edgeTriplet.srcId}\t${edgeTriplet.dstId}", edgeTriplet.attr) // 原来有bug，这两个id得分开，否则(1, 12), (11, 2)key相同
    }.repartition(200).cache
    edge2attr.first

    for (iter <- 0 until config.numWalks) {
      var prevWalk: RDD[(Long, ArrayBuffer[Long])] = null
      var randomWalk: RDD[(VertexId, ArrayBuffer[VertexId])] = graph.vertices.map { case (nodeId, clickNode: NodeAttr) =>
        val pathBuffer = new ArrayBuffer[Long]()
        pathBuffer.append(clickNode.path:_*)
        (nodeId, pathBuffer) 
      }.filter{ case (nodeId, pathBuffer) => pathBuffer.nonEmpty }.cache
      var activeWalks = randomWalk.first
      // unpersist是为了重新生成图
      // 每次iter的时候，图都要重新init， 如果不释放cache，初始的图都是一样的了
      // clickNode.path = Array(vertexId, clickNode.neighbors(nextNodeIndex)._1)  有一个随机的参数nextNodeIndex
      graph.unpersist(blocking = false)
      graph.edges.unpersist(blocking = false)
      
      for (walkCount <- 0 until config.walkLength) {
        prevWalk = randomWalk
        randomWalk = randomWalk
            .map { case (srcNodeId, pathBuffer) =>
                val prevNodeId = pathBuffer(pathBuffer.length - 2)
                val curNodeId = pathBuffer.last

                (s"$prevNodeId\t$curNodeId", (srcNodeId, pathBuffer))
            }
            .join(edge2attr)
            .map { case (edge, ((srcNodeId, pathBuffer), edgeAttr)) =>
                  try {
                      if (pathBuffer != null
                          && pathBuffer.nonEmpty
                          && edgeAttr.dstNeighbors != null
                          && edgeAttr.dstNeighbors.nonEmpty) {
                        val nextNodeIndex: Int = GraphOps.drawAlias(edgeAttr.J, edgeAttr.q)
                        val nextNodeId: VertexId = edgeAttr.dstNeighbors(nextNodeIndex)
                        pathBuffer.append(nextNodeId)
                      }
                    (srcNodeId, pathBuffer)
                  } catch {
                    case e: Exception => throw new RuntimeException(e.getMessage)
                  }
            }.cache

        activeWalks = randomWalk.first() // stage
        /*
        https://www.cnblogs.com/wq3435/p/10975165.html
        大体之意是根据GraphX中Graph的不变性，对g做操作并赋回给g之后，
        g已不是原来的g了，而且会在下一轮迭代使用，所以必须cache。
        另外，必须先用prevG保留住对原来图的引用，并在新图产生后，快速将旧图彻底释放掉。否则，十几轮迭代后，会有内存泄漏问题，很快耗光作业缓存空间。
         */
        prevWalk.unpersist(blocking=false)
      }

      if (randomWalkPaths != null) {
        val prevRandomWalkPaths = randomWalkPaths
        randomWalkPaths = randomWalkPaths.union(randomWalk).cache()
        randomWalkPaths.first
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        randomWalkPaths = randomWalk
      }
      /*
        一个stage的所有task都执行完毕之后，会在各个节点本地的磁盘文件中写入计算中间结果
        当我们在代码中执行了cache/persist等持久化操作时，根据我们选择的持久化级别的不同，每个task计算出来的数据也会保存到Executor进程的内存或者所在节点的磁盘文件中。

        所以这里清除了留在内存中的randomWalk
        randomWalk.first()产生的中间结果仍然保存在节点本地的磁盘文件
         */
      randomWalk.unpersist(blocking = false)
    }
    
    this
  }
  
  def embedding(): this.type = {
    val randomPaths: RDD[Iterable[String]] = randomWalkPaths.map { case (vertexId, pathBuffer) =>
      Try(pathBuffer.map(_.toString).toIterable).getOrElse(null)
    }.filter(_!=null)
    
    Word2vec.setup(context, config).fit(randomPaths)
    
    this
  }
  
  def save(): this.type = {
    this.saveRandomPath()
        .saveModel()
        .saveVectors()
  }
  
  def saveRandomPath(): this.type = {
    randomWalkPaths
            .map { case (vertexId, pathBuffer) =>
              Try(pathBuffer.mkString("\t")).getOrElse(null)
            }
            .filter(x => x != null && x.replaceAll("\\s", "").length > 0)
            .repartition(200)
            .saveAsTextFile(config.output)
    
    this
  }
  
  def saveModel(): this.type = {
    Word2vec.save(config.output)
    
    this
  }
  
  def saveVectors(): this.type = {
    val node2vector = context.parallelize(Word2vec.getVectors.toList)
            .map { case (nodeId, vector) =>
              (nodeId.toLong, vector.map(_.formatted("%.6f")).mkString(","))
            }
    
    if (this.node2id != null) {
      val id2Node = this.node2id.map{ case (strNode, index) =>
        (index, strNode)
      }
      
      node2vector.join(id2Node)
              .map { case (nodeId, (vector, name)) => s"$name\t$vector" }
              .repartition(200)
              .saveAsTextFile(s"${config.output}.emb")
    } else {
      node2vector.map { case (nodeId, vector) => s"$nodeId\t$vector" }
              .repartition(200)
              .saveAsTextFile(s"${config.output}.emb")
    }
    
    this
  }
  
  def cleanup(): this.type = {
    node2id.unpersist(blocking = false)
    indexedEdges.unpersist(blocking = false)
    indexedNodes.unpersist(blocking = false)
    graph.unpersist(blocking = false)
    randomWalkPaths.unpersist(blocking = false)
    
    this
  }

  def loadNode2Id(node2idPath: String): this.type = {
    try {
      this.node2id = context.textFile(config.nodePath).map { node2index =>
        val Array(strNode, index) = node2index.split("\\s")
        (strNode, index.toLong)
      }
    } catch {
      case e: Exception => logger.info("Failed to read node2index file.")
      this.node2id = null
    }

    this
  }
  
  def readIndexedGraph(tripletPath: String) = {
    val bcWeighted = context.broadcast(config.weighted)
    
    val rawTriplets = context.textFile(tripletPath)
    if (config.nodePath == null) {
      this.node2id = createNode2Id(rawTriplets.map { triplet =>
        val parts = triplet.split("\\s")
        (parts.head, parts(1), -1)
      })
    } else {
      loadNode2Id(config.nodePath)
    }

    rawTriplets.map { triplet =>
      val parts = triplet.split("\\s")
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      (parts.head.toLong, parts(1).toLong, weight)
    }
  }
  

  def indexingGraph(rawTripletPath: String): RDD[(Long, Long, Double)] = {
    val rawEdges = context.textFile(rawTripletPath).map { triplet =>
      val parts = triplet.split("\\s")

      Try {
        (parts.head, parts(1), Try(parts.last.toDouble).getOrElse(1.0))
      }.getOrElse(null)
    }.filter(_!=null)
    
    this.node2id = createNode2Id(rawEdges)
    
    rawEdges.map { case (src, dst, weight) =>
      (src, (dst, weight))
    }.join(node2id).map { case (src, (edge: (String, Double), srcIndex: Long)) =>
      try {
        val (dst: String, weight: Double) = edge
        (dst, (srcIndex, weight))
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null).join(node2id).map { case (dst, (edge: (Long, Double), dstIndex: Long)) =>
      try {
        val (srcIndex, weight) = edge
        (srcIndex, dstIndex, weight)
      } catch {
        case e: Exception => null
      }
    }.filter(_!=null)
  }
  
  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]): RDD[(String, Long)] = triplets.flatMap { case (src, dst, weight) =>
    Try(Array(src, dst)).getOrElse(Array.empty[String]) // 输出 src dst 两行
  }.distinct().zipWithIndex()
  
}
