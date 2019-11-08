// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program.
val inputFilePath  = "sample_input.txt"
val outputDirPath = "Output"

val storage_value = 1024
val zeroVal = 0

//min max template
//https://backtobazics.com/big-data/spark/apache-spark-aggregatebykey-example/
//Defining Seqencial Operation and Combiner Operations
//Sequence operation : Finding Maximum payloads from a single partition
def seqOpMax = (accumulator: Int, element: Int) =>
    if(accumulator > element) accumulator else element

//Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def combOpMax = (accumulator1: Int, accumulator2: Int) =>
    if(accumulator1 > accumulator2) accumulator1 else accumulator2


//Defining Seqencial Operation and Combiner Operations
//Sequence operation : Finding Minimum payloads from a single partition
def seqOpMin = (accumulator: Int, element: Int) =>
    if(accumulator < element) accumulator else element

//Combiner Operation : Finding Minimum payloads out Partition-Wise Accumulators
def combOpMin = (accumulator1: Int, accumulator2: Int) =>
    if(accumulator1 < accumulator2) accumulator1 else accumulator2




// Read file: load csv file to RDD
val csv = sc.textFile(inputFilePath,1)


//remove the redundant columns
val d1 = csv.map(line => line.split(","))

//uniform the units in Byte: a list of (key, value) pairs
val d2 = d1.map(x => if(x(3).takeRight(2)== "MB") (x(0),x(3).dropRight(2).toInt*storage_value*storage_value) else if(x(3).takeRight(2)== "KB") (x(0),x(3).dropRight(2).toInt*storage_value)else(x(0),x(3).dropRight(1).toInt))

val d3 = d1.map(x => (x(0),1))

//Base URL, minimum payload, maximum payload, mean payload, variance of payload

val min = d2.aggregateByKey(Int.MaxValue)(seqOpMin, combOpMin)

val minString = min.map(x => (x._1,(x._2.toString+ "B")))

val max = d2.aggregateByKey(zeroVal)(seqOpMax, combOpMax)

val maxString = max.map(x => (x._1,(x._2.toString+ "B")))

val min_max = minString.join(maxString)


val sum = d2.aggregateByKey(zeroVal)(_+_, _+_)
val occurrence = d3.aggregateByKey(zeroVal)(_+_, _+_)

val mean = sum.join(occurrence).map(x => (x._1,(x._2._1/x._2._2).toInt)).map(x => (x._1,(x._2.toString+ "B")))


val min_max_mean = min_max.join(mean).map(x => (x._1,(x._2._1._1,x._2._1._2,x._2._2)))

val variance = d2.groupByKey().map{case(k, v) => {
      //var buffer = new List[Double]

        var count = v.size
        var sum = v.sum
        var mean = sum / count
        var error = 0.0
        v.foreach(w => {
          error += Math.pow(Math.abs(w - mean), 2)
        })
        error /= count
        var outV = error.toLong.toString + "B"
        (k, outV)
}}



val tmpOutput = min_max_mean.join(variance).sortByKey(true)

val finalOutput = tmpOutput.map(x => (x._1,x._2._1._1,x._2._1._2,x._2._1._3,x._2._2))

finalOutput.foreach(println)
finalOutput.saveAsTextFile(outputDirPath) 

 
