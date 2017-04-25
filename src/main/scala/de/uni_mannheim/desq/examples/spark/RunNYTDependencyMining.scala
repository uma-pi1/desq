package de.uni_mannheim.desq.examples.spark

import de.uni_mannheim.desq.experiments.fimi.RunFimi
import de.uni_mannheim.desq.mining.DesqDfs




class SyntaxTree{

  def depthTree(x:Int,recurse:String):String={
    if(x==1){
      if(recurse=="Y"){
        return "rel node < >"
      }
      else{
        return ".*(rel node < >)"
      }
    }
    if(x==2){
      if(recurse=="Y"){
        return "rel node < ["+depthTree(x-1,"Y")+"]+ >"
      }
      else{
        return ".*(rel node < ["+depthTree(x-1,"Y")+"]+ >)"
      }
    }
    var str=".*(rel node < [ ["
    var i=0
    for( i <-1 to x-1){
      if(i==x-1){
        str=str+"]*"+depthTree(i,"Y")+"]+ >)"
      }
      else{
        str=str+depthTree(i,"Y")
        if(i!=x-2){
          str=str+"|"
        }
      }
    }
    if(recurse=="Y"){
      return str.substring(3,str.length-1)
    }
    else{
      return  str
    }

  }
}

/**
  * Created by ryan on 22.04.17.
  */
object RunNYTDependencyMining extends App {
  val dataPath=args(0)
  val dictPath=args(1)
  //val D1 = "rel node < >"
  //val patternExpression=".*(rel node < [rel node < >]+ >)"
  //val D2="rel node < ["+D1+"]+ >"
  //val D3=".*(rel node < [ ["+D1+"]*"+D2+"]+ > )"
  val ob=new SyntaxTree()
  val patEx=ob.depthTree(2,"NY")
  val sigma = 2
  val conf = DesqDfs.createConf(patEx, sigma)
  conf.setProperty("desq.mining.prune.irrelevant.inputs", true)
  conf.setProperty("desq.mining.use.two.pass", true)
  RunFimi.implementFimi(conf,dataPath,dictPath);
}
