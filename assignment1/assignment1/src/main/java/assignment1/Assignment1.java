package assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class Assignment1 {
	
	
	public static class NgramMapper extends Mapper<Object, Text, Text, FilenameIntWritable>{
		
	    private final static IntWritable one = new IntWritable(1);	// default frequency = 1
	    
	   
	    private Text word;				 	// output key
	    private Text filename;		 	// output value filename
	    private Configuration conf;			// global configuration
	    private int Ngram;					// argument Ngram
		
	    /**
	     * 
	     * Called once at the beginning of the task.
	     * 
	     * Initialize the fields
	     * */
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			word = new Text();
			filename = new Text();	
			//get Ngram from configuration
			conf = context.getConfiguration();
			setNgram(Integer.parseInt(conf.get("Ngram")));
			
			//Get filename
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filename.set(fileSplit.getPath().getName());
		}
		
		/**
		 * Called once for each key/value pair in the input split. Most applications
		 * should override this, but the default is the identity function.
		 * 
		 * (key,value) : (Text, {Text, IntWritable})
		 */
		@Override
		public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
			
			//Get tokens from input and form a ArrayList
			StringTokenizer itr = new StringTokenizer(value.toString());
			ArrayList<String> tokenList = new ArrayList<String>();
			while (itr.hasMoreTokens()) {
				tokenList.add(itr.nextToken());
			}
			
			int tokenNum = tokenList.size();
			
			// if Ngram is greater than size of input tokens
			// do nothing
			if(tokenNum < Ngram ) {
				//context.write(word, new FilenameIntWritable());
			}
			// otherwise map the pairs
			else{
				String tmp = "";
				for(int i=0; i < tokenNum - Ngram;i++) {
					tmp = "";
					for(int j = 0; j < Ngram; j++) {
						tmp += tokenList.get(i+j);
						if(j != Ngram-1) tmp += " ";
					}
					word.set(tmp); 
					
					context.write(word, new FilenameIntWritable(one,filename));
				}
				
				
				tmp = "";
				for(int i=0; i < Ngram;i++) {
					tmp += tokenList.get(tokenNum - Ngram + i);
					if(i != Ngram-1) tmp += " ";
				}
				
				word.set(tmp);
				context.write(word, new FilenameIntWritable(one,filename));
				
			}   
	    }
	    
	    
	    /**
	     * 
	     * Called once at the end of the task.
	     * 
	     * */
		@Override
		public void cleanup(Context context ) 
				throws IOException, InterruptedException {
			// to do
		}
		
		public void setNgram(int ngram) {
			Ngram = ngram;
		}

	}
	
	
	public static class NgramCombiner extends Reducer<Text,FilenameIntWritable,Text,FilenameIntWritable> {
		
		private Configuration conf;				// global configuration
		private int minCount;					// argument: minCount
 
		public void reduce(Text key, Iterable<FilenameIntWritable> values,
	                       Context context)throws IOException, InterruptedException {
			
			//initialization
			conf = context.getConfiguration();
			setMinCount(Integer.parseInt(conf.get("minCount")));
			
			
			int sum = 0;
			String sumFile = "";
			//get the total occurrence and append the filenames
			for (FilenameIntWritable val : values) {
				sum += val.getFrequency().get();
				if(!sumFile.contains(val.getFilename().toString())) 
					sumFile += val.getFilename().toString() + " ";
				
			} 
			
			context.write(key, new FilenameIntWritable(new IntWritable(sum),new Text(sumFile)) );
			
		}

		public int getMinCount() {
			return minCount;
		}

		public void setMinCount(int minCount) {
			this.minCount = minCount;
		}
	}
	
	
	
	
	

	
	public static class NgramReducer extends Reducer<Text,FilenameIntWritable,Text,FilenameIntWritable> {
		
		private Configuration conf;				// global configuration
		private int minCount;					// argument: minCount
 
		public void reduce(Text key, Iterable<FilenameIntWritable> values,
	                       Context context)throws IOException, InterruptedException {
			
			//initialization
			conf = context.getConfiguration();
			setMinCount(Integer.parseInt(conf.get("minCount")));
			
			
			int sum = 0;
			String sumFile = "";
			//get the total occurrence and append the filenames
			for (FilenameIntWritable val : values) {
				sum += val.getFrequency().get();
				if(!sumFile.contains(val.getFilename().toString())) 
					sumFile += val.getFilename().toString() + " ";
				
			} 
			
			//output only the pairs with occurrence greater than minimal count
			if(sum >= getMinCount())
				context.write(key, new FilenameIntWritable(new IntWritable(sum),new Text(sumFile)) );
			
		}

		public int getMinCount() {
			return minCount;
		}

		public void setMinCount(int minCount) {
			this.minCount = minCount;
		}
	}

  /**
   * 
   * Source Code: WordCount v2.0
   * 
   * http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v1.0
   * 
   * */
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
    
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    
		//copy arguments as a string list
		String[] remainingArgs = optionParser.getRemainingArgs();
    
    
	    //Provides access to configuration parameters
	    conf.set("Ngram", remainingArgs[0]);
		conf.set("minCount", remainingArgs[1]);
	   
		
		/*
	     * args[0]: Ngram
	     * args[1]: Minimum Count
	     * args[2]: The directory containing the files in input
	     * args[3]: The directory where the output file will be stored.
	     * */
	    
	    if ( remainingArgs.length != 4 ) {
	        System.err.println("Usage: Assignment <Ngram> <MinCount> <IN> <OUT>");
	        System.exit(2);
	    }
	    
	    //create a job
	    Job job = Job.getInstance(conf, "assignment1");
	    job.setJarByClass(Assignment1.class);
	    
	    // map -> combiner -> reducer
	    job.setMapperClass(NgramMapper.class);
	    job.setCombinerClass(NgramCombiner.class);
	    job.setReducerClass(NgramReducer.class);
	    
	    //output(key, value)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FilenameIntWritable.class);
	    
	    //input and output local directory 
	    FileInputFormat.addInputPath(job, new Path(remainingArgs[2]));
	    FileOutputFormat.setOutputPath(job, new Path(remainingArgs[3]));
	    
	    //Submit the job to the cluster and wait for it to finish
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
 
	}
}


/**
 * This is a custom writable class implements the Writable interface.
 * 
 * A serializable object which implements a simple, efficient, serialization 
 * protocol, based on {@link DataInput} and {@link DataOutput}.
 * 
 * The object of this class is used to represent the value of output, 
 * which got two fields in it (filename and frequency).
 *
 * Source code:
 * https://hadoop.apache.org/docs/r2.7.2/api/src-html/org/apache/hadoop/io/Writable.html
 * 
 * */
class FilenameIntWritable implements Writable {
	
	private Text filename;
	private IntWritable frequency;
	
	//constructor: create a default object
	// filename: null
	// frequency: 0
	public FilenameIntWritable() {
		filename = new Text();
		frequency = new IntWritable(0);
		 
	}

	//constructor: create a custom object
	public FilenameIntWritable(IntWritable frequency, Text filename) {
		 
		this.filename = filename;
		this.frequency =  frequency;
	}

	
	
	public void readFields(DataInput arg0) throws IOException {
		filename.readFields(arg0);
		frequency.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		filename.write(arg0);
		frequency.write(arg0);
	}
	
	@Override
	public String toString() {
		return frequency.get() + "\t" + filename.toString();
	}
 
	//getter and setter for fields
	
	public Text getFilename() {
		return filename;
	}

	public void setFilename(Text filename) {
		this.filename = filename;
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}
	
}