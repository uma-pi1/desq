import java.io.{File, PrintWriter}
import java.util.Collections

import de.uni_mannheim.desq.converters.netflix.MovieRating
import de.uni_mannheim.desq.dictionary.DefaultDictionaryAndSequenceBuilder
import it.unimi.dsi.fastutil.ints.IntArrayList

import scala.collection.mutable
import scala.io.Source

class SpmfNetflix extends DefaultDictionaryAndSequenceBuilder{

  def readData(inputFile:String ): Unit ={
    val storePath = inputFile.substring(0, inputFile.lastIndexOf('/') + 1)
    val fileWriter=new PrintWriter(new File(storePath+"test.del"))
    val sequence = new mutable.ArrayBuffer[String]

    dict.addItem(Integer.MAX_VALUE, "@")
    dict.addItem(Integer.MAX_VALUE - 1, "movie")
    val data = new mutable.ArrayBuffer[String]
    val dataItems =new IntArrayList()
    newSequence()
    for(line<- Source.fromFile(inputFile).getLines){
      val sids=line.split("\t")
      for(sid<-sids){
        val pair = appendItem(sid)
        if (pair.getRight) addParent(pair.getLeft, "movie")
      }
      newSequence()
    }



    val items = new mutable.ArrayBuffer[String]
    var custSet = scala.collection.mutable.Set[String]()
    var lookup=scala.collection.mutable.Map[String, String]()

    //precomputation for grouping items into itemsets
    val sequenceIt = MovieRating.getMovieRatingsByUserIterator
    while(sequenceIt.hasNext){
      var userBuff=sequenceIt.next()
      items.append(userBuff(0).userId.toString)
      var buff=new StringBuilder

      for(movieRating <- userBuff){

        buff.append(movieRating.movieId.toString+"@"+movieRating.date+",")
      }
      lookup+=(userBuff(0).userId.toString->buff.toString())
    }


    var count = 0

    for(line<- Source.fromFile(inputFile).getLines){
      var sids=line.split("\t")
      sids=sids:+("end")
      var sidCheck=0
      var sidCount=sids.length
      val customerId=items(count)
      count=count+1
      var demoSeq=sids(0)
      val custSeq=lookup(customerId)
      var dateprev=custSeq.substring(custSeq.indexOf("@",custSeq.indexOf(demoSeq+"@"))+1,custSeq.indexOf(",",custSeq.indexOf(demoSeq+"@")))
      var newdate=" "
      var seqIt=sids.iterator
      while(seqIt.hasNext){

        var sid=seqIt.next()
        if(sid=="end"){
          newdate="end"
        }
        else{
          newdate=custSeq.substring(custSeq.indexOf("@",custSeq.indexOf(sid+"@"))+1,custSeq.indexOf(",",custSeq.indexOf(sid+"@")))
          sidCheck=sidCheck+1
        }


        if(newdate==dateprev){
          data.append(sid)

          dateprev=newdate
        }
        else{
          for(sd<-data){
            dataItems.add(dict.fidOf(sd))
          }
          Collections.sort(dataItems )
          Collections.reverse(dataItems)
          val it=dataItems.iterator()
          while ( it.hasNext()){
            val printNext=it.next()
            fileWriter.print(dict.gidOf(printNext)+" ")
          }
          if(sidCheck+1!=sidCount){
            fileWriter.print(dict.gidOf("@")+" ")
            currentFids.add(dict.fidOf("@"))
            //fileWriter.print("-1 ")
          }

          dataItems.clear()
          data.clear()
          if(sid!="end"){
            data.append(sid)
            dateprev=newdate
          }

        }

      }
      fileWriter.print("\n")
      newSequence()
    }
    dict.recomputeFids
    dict.write(storePath+"test.json")
  }
}



//argument->input file
object RunConverter{
  def main(args: Array[String]): Unit ={
    val inputFile=args(0)
    val ob=new SpmfNetflix()
    ob.readData(inputFile)
  }
}/**
  * Created by ryan on 09.03.17.
  */

