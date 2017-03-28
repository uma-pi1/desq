import java.io.{File, IOException, PrintWriter}
import java.util.Collections

import de.uni_mannheim.desq.converters.netflix.MovieRating
import de.uni_mannheim.desq.dictionary.DefaultDictionaryAndSequenceBuilder
import it.unimi.dsi.fastutil.ints.IntArrayList

import scala.collection.mutable
import scala.io.Source
import util.control.Breaks._

class SpmfNetflix extends DefaultDictionaryAndSequenceBuilder{

  def readData(inputFile:String,rowLimit:Int ): Unit ={
    val storePath = inputFile.substring(0, inputFile.lastIndexOf('/') + 1)
    val fileWriter=new PrintWriter(new File(storePath+"netflixComplete.del"))
    val spmfWriter=new PrintWriter(new File(storePath+"spmfComplete.del"))
    val sequence = new mutable.ArrayBuffer[String]
    println("Building the dictionary......")
    dict.addItem(Integer.MAX_VALUE, "@")
    dict.addItem(Integer.MAX_VALUE - 1, "movie")
    val data = new mutable.ArrayBuffer[String]
    val dataItems =new IntArrayList()
    newSequence()
    var counter=0
    breakable{
      for(line<- Source.fromFile(inputFile).getLines){
        counter=counter+1
        //println(counter)
        val sids=line.split("\t")
        for(sid<-sids){
          val pair = appendItem(sid)
          if (pair.getRight) addParent(pair.getLeft, "movie")
        }
        newSequence()
        if(counter==rowLimit) break
    }
    }
    dict.recomputeFids
    println("Computed initial dictionary, Now preprocessing.....")

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

    println("Preprocessing completed, Converting Data into sequence of itemsets.....")
    var count = 0
    counter=0
    breakable{
      for(line<- Source.fromFile(inputFile).getLines){
        counter=counter+1
        //println(counter)
        var sids=line.split("\t")
        //println(line.toString())
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
              //only add below line if converting to Spmf format
              spmfWriter.print(dict.gidOf(printNext)+" ")
            }
            if(sidCheck+1!=sidCount){
              fileWriter.print(dict.gidOf("@")+" ")
              //only add below line if converting to Spmf format
              spmfWriter.print("-1 ")
              currentFids.add(dict.fidOf("@"))

            }

            dataItems.clear()
            data.clear()
            if(sid!="end"){
              data.append(sid)
              dateprev=newdate
            }

          }

        }
        newSequence()
        //only add below line if converting to Spmf data format
        spmfWriter.print("-1 -2")
        if(counter==rowLimit) break
        fileWriter.print("\n")
        //only add below line if converting to Spmf data format
        spmfWriter.print("\n")
      }

    }
    fileWriter.flush()
    fileWriter.close()
    spmfWriter.flush()
    spmfWriter.close()
    dict.recomputeFids
    dict.write(storePath+"netflixComplete.json")
  }

//removes all users who rate greater than n movies per day
  def extractUsers(inputFile:String,maxRateLimit:Int):Unit={
    val storePath = inputFile.substring(0, inputFile.lastIndexOf('/') + 1)
    val fileWriter=new PrintWriter(new File(storePath+"netflixCompressed"+maxRateLimit+".del"))
    //adding separator and movie item to the dictionary
    //note:movie item is the parent to all items
    println("Filtering Users.....")
    dict.addItem(Integer.MAX_VALUE, "@")
    dict.addItem(Integer.MAX_VALUE - 1, "movie")
    newSequence()
    for(line<- Source.fromFile(inputFile).getLines){
      var sequenceSet=line.trim.split(dict.gidOf("@").toString)
      var flag=false
      breakable{
        for(ratings<-sequenceSet){
          var rating=ratings.trim.split(" ")
          //checking for if user rated > n moveis for a particular date
          if(rating.length>maxRateLimit){
            flag=true
            break
          }
        }
      }

      if(flag==false){
        fileWriter.println(line)
        var sids=line.trim.split(" ")
        for(sid<-sids){
          if(sid==dict.gidOf("@").toString){
            appendItem("@")
          }
          else{
            val pair = appendItem(sid)
            if (pair.getRight) addParent(pair.getLeft, "movie")
          }

        }
        newSequence()
      }
    }
    fileWriter.flush()
    fileWriter.close()
    dict.recomputeFids
    dict.write(storePath+"netflixCompressed"+maxRateLimit+".json")
  }

  def extractUsersSpmf(inputFile:String,maxRateLimit:Int):Unit={
    val storePath = inputFile.substring(0, inputFile.lastIndexOf('/') + 1)
    val spmfWriter=new PrintWriter(new File(storePath+"SpmfCompressed"+maxRateLimit+".del"))
    println("Filtering Users(spmf format).....")
    for(line<- Source.fromFile(inputFile).getLines){
      var sequenceSet=line.trim.split("-1")
      var flag=false
      breakable{
        for(ratings<-sequenceSet){
          var rating=ratings.trim.split(" ")
          //checking for if user rated > n moveis for a particular date
          if(rating.length>maxRateLimit){
            flag=true
            break
          }
        }
      }

      if(flag==false){
        spmfWriter.println(line)
      }
    }
    spmfWriter.flush()
    spmfWriter.close()
  }

}




//argument->input file, no. of lines from data needed
object RunConverter extends App{
    if(args.length<1){
      println("Please enter valid set of arguments")
      println("input file, no. of lines from data needed")
      throw new IllegalArgumentException
    }
    else if(args.length>2){
      println("Too many arguments")
      throw new IllegalArgumentException
    }
    val inputFile=args(0)
    var rowLimit=480189
    if(args.length==2){
      rowLimit=Integer.parseInt(args(1))
    }
    val ob=new SpmfNetflix()
    ob.readData(inputFile,rowLimit)

}/**
  * Created by ryan on 09.03.17.
  */

//argument->input file, spmfFile, max movies rated by user(other users will be filtered out)
object FilterData_UserRateLimit extends App{
  val inputFile=args(0)
  val spmfFile=args(1)
  val maxRateLimit=Integer.parseInt(args(2))
  val ob=new SpmfNetflix()
  ob.extractUsers(inputFile,maxRateLimit)
  ob.extractUsersSpmf(spmfFile,maxRateLimit)
}

