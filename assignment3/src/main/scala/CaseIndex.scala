//sbt library import (lab4)
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

//scala http request library (lab6)
import scalaj.http._

//java File library
import java.io.File

//scala xml file library
import scala.xml._
import play.api.libs.json._

object CaseIndex {
    def main(args: Array[String]) {
         
         val TIME_OUT = 600000       //use 600 seconds to check timeout
         
         
          /********************************************************************/
         /*****  Get all XML files from given path and stored as a list ******/
        /********************************************************************/
        
        // Input path given by arguments
        val inputFile = args(0)
      
        //use the listFiles method of the Java File class to get the directory
        val dir = new File(inputFile)
        
        //get all XML files from the directory
        val xmlFiles = if (dir.exists && dir.isDirectory) 
                            dir.listFiles.filter(_.isFile).toList 
                        else 
                            List[File]()
 
          /********************************************************************/
         /*****             Creat index in elasticsearch                ******/
        /********************************************************************/
        
        //create index named legal_idx
        var indexResponse = Http("http://localhost:9200/legal_idx")
                                .method("PUT")
                                .header("Content-Type", "application/json")
                                .option(HttpOptions.readTimeout(TIME_OUT))
                                .asString
        
        //if the index exists remove it and create a new one
        if(indexResponse.code != 200){
            indexResponse = Http("http://localhost:9200/legal_idx")
                                .method("DELETE")
                                .header("Content-Type", "application/json")
                                .option(HttpOptions.readTimeout(TIME_OUT))
                                .asString
                                
            indexResponse = Http("http://localhost:9200/legal_idx")
                                .method("PUT")
                                .header("Content-Type", "application/json")
                                .option(HttpOptions.readTimeout(TIME_OUT))
                                .asString
        }
        
        println("index response: " + indexResponse)
        
        
          /********************************************************************/
         /*****         Creat type and schema in elasticsearch          ******/
        /********************************************************************/
        
        // XML:
        //     <case>
        //          <name> </name>
        //          <AustLII> </AustLII>
        //          <catchphrases> </catchphrases>
        //          <sentences> </sentences>
        //     </case>
       
        // elastic:
        //      index   :  legal_idx
        //      type    :  cases
        //      mapping :  (schema shows as follow)
  
 
        val mappingResponse = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty")
                    .postData("""{"cases":{
                                     "properties":{
                                        "id"            : {"type" : "text"},   
                                        "name"          : {"type" : "text"}, 
                                        "austlii"       : {"type" : "text"},
                                        "catchphrases"  : {"type" : "text"},
                                        "sentences"     : {"type" : "text"},
                                        "person"        : {"type" : "text"},
                                        "location"      : {"type" : "text"},
                                        "organization"  : {"type" : "text"}
                                     }
                                  }}""")
                    .method("PUT")
                    .header("Content-Type", "application/json")
                    .option(HttpOptions.connTimeout(TIME_OUT))
                    .asString
            
           
          println("map response: " + mappingResponse)
          
          /********************************************************************/
         /*****       Data Curation: XML => JSON and enrichment         ******/
        /********************************************************************/
        
        xmlFiles.foreach(file=>{
        
            //only read the XML file
            if(file.getName() contains "xml"){
            
                val tmpFilename = file.getName().dropRight(4)                   // filename without xml suffix
                val tmpXML = XML.loadFile(file)                                 // actual XML file content
                val tmpAustlii = (tmpXML \ "AustLII").text.filter(_ >= ' ')      // URL of the xml
                val tmpName = (tmpXML \ "name").text                            // actual name of the document
                val tmpCatchphase = (tmpXML \ "catchphrases" \ "catchphrase")   // all sub_catchphrases
                var tmpCatchphases = ""                                         // total catchphrases
                val tmpSentence = (tmpXML \ "sentences" \ "sentence")            // all sub_sentences
                var tmpSentences = ""                                           // total sentence
                

                tmpCatchphase.foreach(tmp=>{
                    tmpCatchphases += tmp.text.filter(_ >= ' ')  + " "
                })
                
                tmpSentence.foreach(tmp1=>{
                    tmpSentences += tmp1.text.filter(_ >= ' ').replace("\"","\\\"")
                })
                
                var tmpPersonSet : Set[String] = Set()
                var tmpLocationSet : Set[String] = Set()
                var tmpOrganizationSet : Set[String] = Set()
                
                

                
                
                  /********************************************************************/
                 /*****          data curation with NLP processing              ******/
                /********************************************************************/
                
                //training with caterphrase
                var tmpCaterphraseResponse = Http(s"""http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D""")
                                     .postData(tmpCatchphases)
                                     .method("POST")
                                     .header("Content-Type", "application/json")
                                     .option(HttpOptions.connTimeout(TIME_OUT))
                                     .asString
                                     .body
                
                
                val tmpCaterphraseJSON : JsValue= Json.parse(tmpCaterphraseResponse)
                
                val elements = (tmpCaterphraseJSON \\ "tokens")
                
                elements.foreach(token=>{
                     
                     val tokenList: List[JsValue] = token.as[List[JsValue]]
                     
                     tokenList.foreach(index=>{
                       val entity = (index \\ "ner").mkString("")
                        if( entity contains "PERSON"){
                            tmpPersonSet += (index \\ "originalText").mkString("").replace("\"","")
                        } else if(entity contains  "LOCATION"){
                            tmpLocationSet += (index \\ "originalText").mkString("").replace("\"","")
                        } else if(entity contains  "ORGANIZATION"){
                            tmpOrganizationSet += (index \\ "originalText").mkString("").replace("\"","")
                        }
                     })
                        
                })
                
                
                //training with sentences
                var tmpSentencesResponse = Http(s"""http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D""")
                                     .postData(tmpSentences)
                                     .method("POST")
                                     .header("Content-Type", "application/json")
                                     .option(HttpOptions.connTimeout(TIME_OUT))
                                     .asString
                                     .body
                
                

                val tmpSentencesJSON : JsValue= Json.parse(tmpSentencesResponse)
                
                val elements1 = (tmpSentencesJSON \\ "tokens")
                
                elements1.foreach(token=>{
                     
                     val tokenList: List[JsValue] = token.as[List[JsValue]]
                     
                     tokenList.foreach(index=>{
                       val entity = (index \\ "ner").mkString("")
                        if( entity contains "PERSON"){
                            tmpPersonSet += (index \\ "originalText").mkString("").replace("\"","")
                        } else if(entity contains  "LOCATION"){
                            tmpLocationSet += (index \\ "originalText").mkString("").replace("\"","")
                        } else if(entity contains  "ORGANIZATION"){
                            tmpOrganizationSet += (index \\ "originalText").mkString("").replace("\"","")
                        }
                     })
                        
                })
               
               
                // entities:
                // convert the Set() type to Json List type
                val tmpPerson = "[" + tmpPersonSet.mkString(",") + "]"
                val tmpLocation = "[" + tmpLocationSet.mkString(",") + "]"
                val tmpOrganization = "[" + tmpOrganizationSet.mkString(",") + "]"
                
    
                
                  /********************************************************************/
                 /*****         Creat new document in Elasticsearch             ******/
                /********************************************************************/
                
               val json = s"""{
                            "id"             : "$tmpFilename",
                            "austlii"        : "$tmpAustlii",
                            "name"           : "$tmpName",
                            "catchphrases"   : "$tmpCatchphases",
                            "sentences"      : "$tmpSentences",
                            "person"         : "$tmpPerson",
                            "location"       : "$tmpLocation",
                            "organization"   : "$tmpOrganization"
                            }"""
                
                
               
               val documents = Http("http://localhost:9200/legal_idx/cases/"+tmpFilename+"?pretty")
                                            .postData(json)
                                            .method("PUT")
                                            .header("Content-Type", "application/json")
                                            .option(HttpOptions.readTimeout(TIME_OUT))
                                            .asString
                
                println("HTTP STATUS for file "+ file.getName() + ":")
                println("index response: " + indexResponse.code)
                println("mapping response: " + mappingResponse.code)
                println("documents response: " + documents.code)
                println()
                println()
                 
            }
            
        })        
        
     }
}















