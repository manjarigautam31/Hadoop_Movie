package hadoop.movie;
import hadoop.util.ConfigurationUtil;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
 * A MapReduce application to show which actors played in each movie.  The input data is imdb.tsv.
 * The output data has two columns: movie title and a semicolon separated list of actor names.
 *
 */
public class ShowActorsInMovie extends Configured implements Tool {
  
	private static final Log LOG = LogFactory.getLog(ShowActorsInMovie.class);

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		System.out.println(args.length);
		System.out.println(args[0]);
		System.out.println(args[1]);
		if (args.length != 2) {
			System.err.println("Please enter the input directory & the output directory separated by a space");
			System.err.println("Usage: ShowActorsInMovie <in> <out>");
			System.exit(2);
		}

		ConfigurationUtil.dumpConfigurations(conf, System.out);

		LOG.info("input: " + args[0] + " output: " + args[1]);

		Job job = new Job(conf, "ShowActorsInMovie");
		job.setJarByClass(ShowActorsInMovie.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setNumReduceTasks(1);	
		job.setReducerClass(ActorsListReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ShowActorsInMovie(), args);
		System.exit(exitCode);

	}

	public static class MovieTokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private static Text title = new Text();
		private static Text actor = new Text();


		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\t");

			if (tokens.length == 3) {
				actor.set(tokens[0]);
				title.set(tokens[1]);
				context.write(title, actor);
			}
		}
	}

	public static class ActorsListReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text movieTitle, Iterable<Text> actorNames, Context context) 
				 throws IOException, InterruptedException {

			StringBuffer listOfActors = new StringBuffer();
			String delim = "";
			for (Text name : actorNames) {
				listOfActors.append(delim).append(name.toString());
			    delim = ";";
			}
			context.write(movieTitle, new Text(listOfActors.toString()));
		}
	}

}

