package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.experiments.fimi.RunFimi
import de.uni_mannheim.desq.mining.DesqDfs




class SyntaxTree{

  def depthTree(x:Int):String={
    if(x==0){
      return "< >"
    }
    else{
      return "< [ edge node "+depthTree(x-1)+"]* >"
    }
  }

  /**returns pattern expression to mine sngrams based on the input n*/
  def sng(x:Int,child:String,dmax:String):String={
    var str=path(x-1,child,dmax)
    val skipStr=".*(edge node <)"
    var j=(x-1)/2
    if(j>=1){
      var i=0
      for(i<-1 to j){
        var str1=path(i,child,dmax)
        var str2=path(x-i-1,child,dmax)
        str=str+"|"+str1.substring(0,str1.lastIndexOf("(>)"))+str2.substring(skipStr.length)
        if(i!=x-i-1){
          str=str+"|"+str2.substring(0,str2.lastIndexOf("(>)"))+str1.substring(skipStr.length)
        }
      }
    }
    return str
  }

  /**return path of length y*/
  def path(y:Int,child:String,dmax:String):String={
    if(y==0){
      return "(edge node)"+dmax
    }
    else{
      val temp_str=path(y-1,child,dmax)
      if(y==1){
        return ".*(edge node <)"+"["+child+"]*"+temp_str+"["+child+"]*"+"(>)"
      }
      else{
        return ".*(edge node <)"+"["+child+"]*"+temp_str.substring(".*".length)+"["+child+"]*"+"(>)"
      }
    }
  }

}





/**
  * Created by ryan on 22.04.17.
  */
object RunNYTDependencyMining extends App {
  val dataPath=args(0)
  val dictPath=args(1)
  val ob=new SyntaxTree()
  val maxDepth=5
  val child="edge node "+ob.depthTree(maxDepth)
  val dmax=ob.depthTree(maxDepth)
  //val patEx=".*(node <)["+child+"]*"+"(edge node)"+ob.depthTree(maxDepth)+"["+child+"]*"+"(>)"
  val patEx=ob.sng(2,child,dmax)
  val sigma = 1
  val conf = DesqDfs.createConf(patEx, sigma)
  conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
  conf.setProperty("desq.mining.use.two.pass", true)
  RunFimi.implementFimi(conf,dataPath,dictPath);
}
