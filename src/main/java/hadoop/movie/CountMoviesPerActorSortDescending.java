package hadoop.movie;

import hadoop.util.ConfigurationUtil;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




	/**
	 * 
	 * @author Svetlana Vaz
	 * A MapReduce application to count how many movies each actor have played in. The input data to this homework is imdb.tsv file.
	 * The output data has two columns: count and actor 
	 * The output is sorted by count in descending order
	 * The final output file is placed in the folder called finalOutput
	 *
	 **/


public class CountMoviesPerActorSortDescending extends Configured implements Tool {
	

	private static final Log LOG = LogFactory.getLog(CountMoviesPerActorSortDescending.class);

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println(args[1]);
		if (args.length != 2) {
			System.err.println("Please enter the input directory & the output directory separated by a space");
			System.err.println("Usage: CountMoviesPerActorSortDescending <in> <out>");
			System.exit(2);
		}
		
		ConfigurationUtil.dumpConfigurations(conf, System.out);

		LOG.info("input: " + args[0] + " output: " + args[1]);

		Job job = new Job(conf, "CountMoviesPerActorSortDescending-job1");
		job.setJarByClass(CountMoviesPerActorSortDescending.class);
		
		//map-reduce job 1
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setNumReduceTasks(1);	
		job.setReducerClass(ActorMovieCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String job1Output = new String(args[1]);
		String job2Output = job1Output + "/finalOutput";

		boolean resultFromJob1 = job.waitForCompletion(true);
		
		//send the output of reducer 1 as input to mapper 2
		//map-reduce job 2 to sort the output by count in descending order
		Job job2 = new Job(conf, "CountMoviesPerActorSortDescending-job2");
		job2.setJarByClass(CountMoviesPerActorSortDescending.class);
		job2.setMapperClass(ActorMovieCountMapper.class);
		job2.setNumReduceTasks(1);	
		job2.setReducerClass(ActorMovieCountSortedDescendingReducer.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setSortComparatorClass(DecreasingKeysSort.class);
		
		FileInputFormat.addInputPath(job2, new Path(job1Output + "/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(job2Output));
		
		boolean resultFromJob2 = job2.waitForCompletion(true);
		
		return (resultFromJob1 && resultFromJob2) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CountMoviesPerActorSortDescending(), args);
		System.exit(exitCode);
	}
	
	/*
	 * Map 1: reads each line of the input file and outputs the actor name and a 1
	 */
	public static class MovieTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static IntWritable ONE = new IntWritable(1);
		private static Text actor = new Text();


		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 3) {
				actor.set(tokens[0]);
				context.write(actor, ONE);
			}
		}
	}

	/*
	 * Reducer 1 : Takes as input the actor and intwritable 1, the program sums up all the ones for the actor and writes out the count and the actor as a single entry
	 */
	public static class ActorMovieCountReducer extends Reducer< Text,IntWritable, IntWritable,Text> {
		private static IntWritable count = new IntWritable();
		
		@Override
		public void reduce(Text actor,Iterable<IntWritable> values,  Context context) 
				 throws IOException, InterruptedException {

			int countForActor = 0;
			for (IntWritable count : values) {
				countForActor += count.get();
			}
			count.set(countForActor);
			context.write(count,actor);

		}
	}
	/*
	 * Map 2: input is an count of movies and the actor name and output is the count and actor name.Exactly the same input & output, just interchanged the keys & values
	 */
	public static class ActorMovieCountMapper extends Mapper<Object, Text, IntWritable, Text> {
		private static IntWritable count = new IntWritable();
		private static Text actor = new Text();


		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");
			if (tokens.length == 2) {
				count.set(Integer.parseInt(tokens[0]));
				actor.set(tokens[1]);
				context.write(count, actor);
			} else {
				System.exit(1);

			}
		}
	}

	/*
	 * Reducer 2 : Takes as input the count and actor, the program sorts the count for the actor and writes out the actor and the count in descending order
	 */
	public static class ActorMovieCountSortedDescendingReducer
	extends Reducer<IntWritable, Text, IntWritable, Text> {


		public void reduce(IntWritable key, Text value, OutputCollector<IntWritable, Text> out,Context context)  throws IOException, InterruptedException {
			context.write(key,value);
		}
	}

	/*
	 *  This comparator will sort the keys in descending order. This is required, as by default the reducer sorts its keys in ascending order
	 */
	
	public static class DecreasingKeysSort  extends IntWritable.Comparator {
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
		static {              
			WritableComparator.define(DecreasingKeysSort.class,
					new IntWritable.Comparator());
		}

	}
}




