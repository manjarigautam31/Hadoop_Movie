package hadoop.movie;

import hadoop.movie.CountMoviesPerActorSortDescending;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;


public class CountMoviesPerActorSortDescendingTest {
	
	private static final Log LOG = LogFactory.getLog(CountMoviesPerActorSortDescendingTest.class);
	
	@Test
	public void mapper1Test() throws Exception {
		LOG.info("..... inside mapper1 test");
		
		MapDriver<Object, Text, Text, IntWritable> mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		
		mapDriver.withMapper(new CountMoviesPerActorSortDescending.MovieTokenizerMapper())
		.withInput(new IntWritable(10), new Text("Cooper, Chris (I)	Seabiscuit	2003"))
		.withOutput(new Text("Cooper, Chris (I)"), new IntWritable(1))
		.runTest();
			
		System.out.println("expected output:" + mapDriver.getExpectedOutputs());
	}
	
	@Test
	public void reducer1Test() throws Exception {
		ReduceDriver<Text,IntWritable, IntWritable,Text> reduceDriver =
					new ReduceDriver<Text,IntWritable, IntWritable,Text>();
		
		List<IntWritable> countList = new ArrayList<IntWritable>();
		countList.addAll(Arrays.asList(new IntWritable(2), new IntWritable(3), new IntWritable(4)));
		Text actor = new Text("Cooper, Chris (I)");
		reduceDriver.withReducer(new CountMoviesPerActorSortDescending.ActorMovieCountReducer())
		.withInput(actor, countList)
		.withOutput(new IntWritable(9),actor)
		.runTest();
		
		System.out.println("expected output:" + reduceDriver.getExpectedOutputs());
	}
	
	@Test
	public void mapper2Test() throws Exception {
		LOG.info("..... inside mapper2 test");
		
		MapDriver<Object, Text, IntWritable, Text> mapDriver = new MapDriver<Object, Text, IntWritable, Text>();
		
		mapDriver.withMapper(new CountMoviesPerActorSortDescending.ActorMovieCountMapper())
		.withInput(new IntWritable(9), new Text("9	Cooper, Chris (I)"))
		.withOutput(new IntWritable(9), new Text("Cooper, Chris (I)"))
		.runTest();
			
		System.out.println("expected output:" + mapDriver.getExpectedOutputs());
	}
	
	@Test
	public void reducer2Test() throws Exception {
		ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver =
					new ReduceDriver<IntWritable, Text, IntWritable, Text>();
		
		List<Text> actorList = new ArrayList<Text>();
		actorList.addAll(Arrays.asList(new Text("Cooper, Chris (I)"),new Text("Andrade, Jack"),new Text("Beauchene, Bill")));
		
		reduceDriver.withReducer(new CountMoviesPerActorSortDescending.ActorMovieCountSortedDescendingReducer())
		.withInput(new IntWritable(9), actorList)
		.withOutput(new IntWritable(9), new Text("Cooper, Chris (I)"))
		.withOutput(new IntWritable(9), new Text("Andrade, Jack"))
		.withOutput(new IntWritable(9), new Text("Beauchene, Bill"))
		.runTest();
		
		System.out.println("expected output:" + reduceDriver.getExpectedOutputs());
	}
	
}
