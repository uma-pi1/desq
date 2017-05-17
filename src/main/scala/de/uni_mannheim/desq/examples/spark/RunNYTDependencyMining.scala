package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.experiments.fimi.RunFimi
import de.uni_mannheim.desq.mining.DesqDfs

import scala.collection.JavaConverters._




class SyntaxTree{

  /**Used to skip an entire subtree */
  def depthTree(x:Int):String={
    if(x==0){
      return "< >"
    }
    else{
      return "< [ edge direction node "+depthTree(x-1)+"]* >"
    }
  }

  /**Returns the shortest path between two items */
  def shortestPath(item1:String,item2:String,minDist:Int,maxDist:Int,child:String,dmax:String):String={
    var str=""
    for(i<-minDist to maxDist){
      if(i==minDist)
        str+=".*("+item1+" <)"+dist(i,item2,child,dmax)+"["+child+"]*(>)"+" | "+".*("+item2+" <)"+dist(i,item1,child,dmax)+"["+child+"]*(>)"
      else{
        str+=" | "+".*("+item1+" <)"+dist(i,item2,child,dmax)+"["+child+"]*(>)"+" | "+".*("+item2+" <)"+dist(i,item1,child,dmax)+"["+child+"]*(>)"
      }

      for(j<-1 to i-1){
        str+=" | "+".*(node <)"+dist(j,item1,child,dmax)+dist(i-j,item2,child,dmax)+"["+child+"]*(>)"+" | "+".*(node <)"+dist(j,item2,child,dmax)+dist(i-j,item1,child,dmax)+"["+child+"]*(>)"
      }

    }
    return str
  }

  /**Captures the distance to a random depth from a given node*/
  def dist(d:Int,item:String,child:String,dmax:String):String={
    if(d==1){
      return "["+child+"]*(edge direction "+item+")"+dmax
    }
    else{
      return "["+child+"]*(edge direction node <)"+dist(d-1,item,child,dmax)+"["+child+"]*(>)"
    }
  }

  /**Generates a substructure pattern for a sngram*/
  def genPatEx(str:String,child:String,dmax:String):String={
    var seq:Array[String]=str.split(",")
    var pattern=""
    var childPath=""
    for(i<-0 until seq.length){
      childPath+=dist(seq(i).toInt,"node",child,dmax)+" "
    }
    pattern=".*(node <)"+childPath+"["+child+"]*(>)"
    return pattern
  }

  /**Generates all possible subset sums which are required to generate the sngram*/
  def subsetSum(sum:Int,str:String,child:String,dmax:String):String={
    if(sum==0){
      var tempPath=genPatEx(str.toString(),child:String,dmax:String)
      return tempPath+"|"
    }
    var path=new String
    for(i<-1 to sum){
      var subPath=subsetSum(sum-i,str+i.toString+",",child:String,dmax:String)
      path+=subPath
    }
    return path
  }

  /**Utility function to generate all possible */
  def subsetSumUtility(ngram:Int,child:String,dmax:String): String ={
    val pairsum=ngram-1
    var path=new String
    for(n<-1 to pairsum){
      var str=new String
      str+=n.toString+","
      var subPath=subsetSum(pairsum-n,str,child:String,dmax:String)
      path+=subPath
    }
    return path.substring(0,path.length-1)
  }

}


/**
  * Created by ryan on 22.04.17.
  */
object RunNYTDependencyMining extends App {
  val dataPath=args(0)
  val dictPath=args(1)
  val ob=new SyntaxTree()
  val maxDepth=17
  //child is used to skip a partucular child(along with all its children) of a node
  val child="edge direction node "+ob.depthTree(maxDepth)
  val dmax=ob.depthTree(maxDepth)
  //patEx to find shortest path between 2 items
  val patEx=ob.shortestPath("NN","DT",1,3,child,dmax)
  //patEx to mine syntactic n-grams
  //val patEx=ob.subsetSumUtility(4,child,dmax)
  val sigma = 1
  val conf = DesqDfs.createConf(patEx, sigma)
  conf.setProperty("desq.mining.use.lazy.dfa", true)
  conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
  conf.setProperty("desq.mining.use.two.pass", true)
  RunFimi.implementFimi(conf,dataPath,dictPath);
}


