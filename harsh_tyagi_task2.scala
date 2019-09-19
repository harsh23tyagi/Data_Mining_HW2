import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import java.io.{File, FileWriter, Writer}
import util.control.Breaks._

import org.spark_project.guava.util.concurrent.Striped
object harsh_tyagi_task2 {

  var totalSize = 0
  var threshold = 0
  var inputFile = "test_data.csv"
  var casetype = 1
  var outputfile ="output_task1.txt"
  var freq_list = List[Any]()
  var loopLength = 0
  var mainThreshold = 0


  def sortingSubLists[A : Ordering](coll: List[List[A]]) = coll.sorted[Iterable[A]]

  def phase2(iter: Iterator[(String, Iterable[String])], finalCandidateList: List[Any])={

    var listback = scala.collection.mutable.Map[Any,Int]()
    //    println("Entered Phase 2")
    while(iter.hasNext){
      var tup = iter.next()
      var rowMain = tup._2.toSet

      for( freq <- finalCandidateList){

        if(freq.isInstanceOf[String]){
          if(rowMain.contains(freq.asInstanceOf[String])){

            //            println(temp)
            if(listback.contains(freq)){
              listback(freq)= listback(freq)+1
            }else{
              listback(freq)= 1
            }

            //`yield` (freq,1)
          }
          else {}// `yield`(freq, 0)
        }

        else{

          if((freq.asInstanceOf[List[String]].toSet).subsetOf(rowMain)){

            if(listback.contains(freq)){
              listback(freq)= listback(freq)+1
            }else{
              listback(freq)= 1
            }

          }
        }
      }



    }

    listback.iterator

  }

  def phase1(iter: Iterator[(String, Iterable[String])]) =
  {
    //    Phase 1 of the code
    var rowList = List[List[String]]()

    var localfreq = List[Any]()
    var rowSetData = List[Set[String]]()

    var k =0
    while(iter.hasNext){
      var tup = iter.next()
      rowList =  rowList :+ tup._2.toList
      rowSetData = rowSetData :+ tup._2.toSet

    }

    val chunkSize =  rowList.length
    val fraction = (chunkSize.asInstanceOf[Float]/totalSize.asInstanceOf[Float])

    val chunkThreshold = threshold * fraction
    //    print(chunkThreshold+"\t"+chunkSize+"\t"+fraction)

    //    chunkThreshold calculated

    //    calculating singletons

    var countDict = scala.collection.mutable.Map[Any, Float]()
    for (row <- rowList){
      for (candidate <- row){
        if(countDict.contains(candidate)){
          countDict(candidate) = countDict(candidate)+1
        }
        else{
          countDict(candidate) =1
        }

      }
    }

    var candidate = List[String]()
    var cd = List[Any]()
    for ((k,v) <- countDict){
      if (v >= chunkThreshold){
        candidate = candidate :+ k.toString()

      }
    }

    if(candidate.length == 0){
      val ita = Iterator(countDict)
      ita
    }
    else{
      candidate = candidate.sorted
      localfreq = localfreq ::: candidate

    }



    loopLength = (localfreq.length)


    //  -------------------------------------- ::::::Starting to Create Pairs:::::: --------------------------------------


    var tuples = List[List[String]]()
    tuples = candidate.combinations(2).toList

    var countDict_tup = scala.collection.mutable.Map[List[String], Float]()
    var freq = List[List[String]]()
    //checking for pairs



    //    ---------------------------with Sets--------------------//
    for (row <- rowSetData){
      for (candidate <- tuples){
        if(candidate.toSet.subsetOf(row)){
          if(countDict_tup.contains(candidate)){
            countDict_tup(candidate) = countDict_tup(candidate)+1
          }
          else{
            countDict_tup(candidate) =1
          }
        }
      }
    }

    for ((k,v) <- countDict_tup){
      if (v >= chunkThreshold){
        freq = freq :+ k.sorted

      }
    }
    localfreq = localfreq ::: freq


    ////  -------------------------------------- ::::::Looping for itemset of size k:::::: --------------------------------------
    //
    ////---------Loop Begins------------------------

    var temp = List[String]()
    var i = 2
    while(freq.length != 0 && i<loopLength){

      //    println(i)

      tuples = List[List[String]]()
      temp = List[String]()

      for (k <- 0 to freq.length-2){
        for(j <- (k+1) to freq.length-1){
          if(freq(k).slice(0, i-1) == freq(j).slice(0, i-1)){
            temp = (freq(k) ++ freq(j)).sorted.distinct
            tuples = tuples :+ temp
          }


        }
      }
      localfreq = localfreq ::: tuples
      i=i+1

      //
      //

      countDict_tup = countDict_tup.empty

      for (row <- rowSetData){
        for (cand <- tuples){
          if(cand.toSet.subsetOf(row)) {
            if (countDict_tup.contains(cand)) {
              countDict_tup(cand) = countDict_tup(cand) + 1
            }
            else {
              countDict_tup(cand) = 1
            }
          }
        }
      }

      freq = List[List[String]]()

      for ((k,v) <- countDict_tup){
        if (v >= chunkThreshold){

          freq = freq :+ k.sorted
        }
      }


      if(freq.length == 0){
        val ita = Iterator(localfreq)
        ita

      }
      else{
        //        println(freq)
        localfreq = localfreq ::: freq

      }

    }
    //

    // Return in the end

    val ita = Iterator(localfreq)
    // return Iterator[U]
    ita
  }


  def writingToFile(list: List[Any], listcandidate: List[Any]): Unit = {



    val f = new File(outputfile);
    f.createNewFile();
    val w: Writer = new FileWriter(f)

    w.write("Candidates:\n")

    var currValue = 1
    var emptyList = List[List[String]]()
    var mainLine = ""
    var sortedList = List[List[String]]()
    var sin_cand = List[String]()


    // Printing Candidate Pairs
    for( item <- listcandidate){

      var item_value = item.asInstanceOf[Tuple2[Any, Int]]._1
      var itemSize = item.asInstanceOf[Tuple2[Any, Int]]._2


      if(itemSize == currValue){

        if(itemSize==1){
          sin_cand =   sin_cand :+ item_value.asInstanceOf[String]
        }
        else{
          emptyList = emptyList :+ item_value.asInstanceOf[List[String]]
        }
      }
      else{
        if(currValue == 1){
          w.write("('" + sin_cand.mkString("'),('") + "')\n\n")
          emptyList =emptyList :+ item_value.asInstanceOf[List[String]]
          currValue = currValue+1
        }else{
          sortedList = sortingSubLists(emptyList)
          mainLine = ("'" + sortedList.mkString("','") + "'\n\n").replace("'List(","('").replace(")'","')").replace(", ","', '")
          w.write(mainLine)
          emptyList = List[List[String]]()
          emptyList =emptyList :+ item_value.asInstanceOf[List[String]]
          //        w.write("\n\n"+item_value.toString().replace("List","").replace("(","('").replace(", ","', '").replace(")","')"))
          currValue = currValue+1
        }
        //mainList = mainList :+ emptyList

      }


    }

    if(emptyList.length != 0){
      sortedList = sortingSubLists(emptyList)
      mainLine = ("'" + sortedList.mkString("','") + "'\n\n").replace("'List(","('").replace(")'","')").replace(", ","', '")
      w.write(mainLine)
    }

    //   Printing Frequent Itemsets-------------------------
    currValue =1
    sin_cand = List[String]()
    w.write("\nFrequent Itemsets:\n")

    for( item <- list){

      var item_value = item.asInstanceOf[Tuple2[Any, Int]]._1
      var itemSize = item.asInstanceOf[Tuple2[Any, Int]]._2


      if(itemSize == currValue){

        if(itemSize==1){
          sin_cand =   sin_cand :+ item_value.asInstanceOf[String]
        }
        else{
          emptyList = emptyList :+ item_value.asInstanceOf[List[String]]
        }
      }
      else{
        if(currValue == 1){
          w.write("('" + sin_cand.mkString("'),('") + "')\n\n")
          emptyList =emptyList :+ item_value.asInstanceOf[List[String]]
          currValue = currValue+1
        }else{
          sortedList = sortingSubLists(emptyList)
          mainLine = ("'" + sortedList.mkString("','") + "'\n\n").replace("'List(","('").replace(")'","')").replace(", ","', '")
          w.write(mainLine)
          emptyList = List[List[String]]()
          emptyList =emptyList :+ item_value.asInstanceOf[List[String]]
          //        w.write("\n\n"+item_value.toString().replace("List","").replace("(","('").replace(", ","', '").replace(")","')"))
          currValue = currValue+1
        }
        //mainList = mainList :+ emptyList

      }


    }

    if(emptyList.length != 0){
      sortedList = sortingSubLists(emptyList)
      mainLine = ("'" + sortedList.mkString("','") + "'\n\n").replace("'List(","('").replace(")'","')").replace(", ","', '")
      w.write(mainLine)
    }


    w.close()

    println("completed")


  }

  def initialization(inputFile: String, casetype: Int, outputfile: String, threshold: Int): Unit ={

    println("Initializing...")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //    var map = sc.parallelize(List("a","ab","abx",4)).map(x=>{x, x.length})
    //    print(map.collect().toList)
    //    return
    val csvread = sc.textFile(inputFile)
    val columnName = csvread.first().toString().split(',').toList


    casetype match{
      case 1 => {
        val items  = csvread.map(line => {line.split(',')}).filter(line => {(line(0) != columnName(0))}).map(line => (line(0), line(1))).groupByKey().filter(x=>{x._2.toList.length > mainThreshold})
        //    print(items.first().toString())
        totalSize = items.keys.collect().toList.length
        val partition = items.mapPartitions(phase1)
        println("Starting with phase 1:")
        val answer = partition.collect().toList
        var finalList: List[Any] = (answer(0) ++ answer(1)).distinct

        val candidateMap = finalList.map(x=>{
          if(x.isInstanceOf[String]){
            (x,1)
          }
          else{
            (x, x.asInstanceOf[List[String]].length)
          }

        }).sortBy(_._2)


        println("Starting with phase 2...")
        val phase2Out = items.mapPartitions(phase2(_,finalList)).reduceByKey(_+_).filter(x=> x._2 >= threshold).map(x=>{
          if(x._1.isInstanceOf[String]){
            (x._1,1)
          }
          else(
            (x._1, x._1.asInstanceOf[List[String]].length)
            )
        }).sortBy(_._2)

        println("Writing to file...")
        //        print(phase2Out.collect().toList(0))

        writingToFile(phase2Out.collect().toList, candidateMap)


      }

      case 2 =>{
        val items  = csvread.map(line => {line.split(',')}).filter(line => {(line(0) != columnName(0))}).map(line => (line(1), line(0))).groupByKey()
        //    print(items.first().toString())
        totalSize = items.keys.collect().toList.length
        val partition = items.mapPartitions(phase1)
        println("Starting with phase 1::")
        val answer = partition.collect().toList
        var finalList: List[Any] = (answer(0) ++ answer(1)).distinct

        val candidateMap = finalList.map(x=>{
          if(x.isInstanceOf[String]){
            (x,1)
          }
          else{
            (x, x.asInstanceOf[List[String]].length)
          }

        }).sortBy(_._2)


        println("Starting with phase 2...")
        val phase2Out = items.mapPartitions(phase2(_,finalList)).reduceByKey(_+_).filter(x=> x._2 >= threshold).map(x=>{
          if(x._1.isInstanceOf[String]){
            (x._1,1)
          }
          else(
            (x._1, x._1.asInstanceOf[List[String]].length)
            )
        }).sortBy(_._2)

        println("Writing to file...")
        //        print(phase2Out.collect().toList(0))

        writingToFile(phase2Out.collect().toList, candidateMap)


      }
    }
  }
  def main(args: Array[String]) {
    inputFile = "test_data.csv"
    threshold = 50
    casetype = 1
    outputfile = "output_task1.txt"
    mainThreshold = 70

    try {
      casetype = 1
      mainThreshold = args(0).toInt
      threshold = args(1).toInt
      inputFile = args(2)
      outputfile = args(3)

    } catch {

      case e: Exception => e.printStackTrace

    }
    var start_time = System.currentTimeMillis()
    initialization(inputFile, casetype, outputfile, threshold)
    var end_time = System.currentTimeMillis()
    var Duration = (end_time-start_time)/1000
    println("\nDuration: "+Duration+"\n")





  }

}


