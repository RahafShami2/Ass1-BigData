package org.InformationRetrieval.spark
import org.apache.spark.{SparkConf, SparkContext}
object InformationRetrieval {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Information Retrieval")
    val sc = new SparkContext(conf)
    // To avoid logging too many debugging messages
    sc.setLogLevel("ERROR")
    //Inverted Index Construction:
    val path="src/main/resources/Files/*"
    // Read all files in the folder
    val rdd = sc.wholeTextFiles(path)
      .flatMap { case (file, text) => {
        // split the words
        val words = text.split("""\W+""").filter(word => word.size > 1)
        //split the file name
        val filename = file.split("/").last.split('.')(0)
        //Create a tuple with count 1 (word, (fileName, 1))
        words.map(w => (w, filename))
      }}
      //reduce file name
      .reduceByKey { case (x, y) => (x + ',' + y) }
      //sorted in alphabetical
      .sortByKey()
      //the number of documents containing Word
      .map { case (w,f) => (w,f.split(",").toSet.size,f.split(",").toSet.toList.sorted.mkString(","))}
      //save Inverted Index result in wholeInvertedIndex.txt file
   .saveAsTextFile("src/main/resources/Files/wholeInvertedIndex.txt")

    //Query Processing:
    //read wholeInvertedIndex.txt file
    val newRdd=sc.textFile("src/main/resources/Files/wholeInvertedIndex.txt")
    //user input to search
    println("Enter a word to search it .. ")
    val search = scala.io.StdIn.readLine()
    //convert string to tuples
    val m= newRdd.map(x=>{
     val y= x.slice(1,x.length-1).split(",")
      (y(0) , (y.slice(2,y.length)))
    })
    val s=search.split(" ")
    //ifthe user entered space(nothing)
    if(s.size == 0){
      println("please try again!")
    }
    //if the user input one word ... returns the word i searched for & in any existing files
    else if(s.size == 1){
       m.filter(x=>{
        if (x._1 == search)
          println(x._2)
        else false
       })
    }
   //if the user input more than one word
    else{
     val w1= m.filter(x=>{
        if (x._1 == s._1) {
          x._2}
        })
     val w2= m.filter(x=>{
          if (x._1 == s._2) {
            x._2}
          })
          w1.intersection(w2).collect
   }
  }
}




