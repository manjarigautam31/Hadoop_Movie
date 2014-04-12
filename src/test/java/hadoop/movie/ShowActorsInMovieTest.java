package hadoop.movie;

import hadoop.movie.ShowActorsInMovie;

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


public class ShowActorsInMovieTest {
	
	private static final Log LOG = LogFactory.getLog(ShowActorsInMovieTest.class);
	
	@Test
	public void mapperTest() throws Exception {
		LOG.info("..... inside mapper test");
		
		MapDriver<Object, Text, Text, Text> mapDriver = new MapDriver<Object, Text, Text, Text>();
		
		mapDriver.withMapper(new ShowActorsInMovie.MovieTokenizerMapper())
		.withInput(new IntWritable(10), new Text("Cooper, Chris (I)	Me, Myself & Irene	2000"))
		.withOutput(new Text("Me, Myself & Irene"), new Text("Cooper, Chris (I)"))
		.runTest();
			
		System.out.println("expected output:" + mapDriver.getExpectedOutputs());
	}
	
	@Test
	public void reducerTest() throws Exception {
		ReduceDriver<Text, Text, Text, Text> reduceDriver =
					new ReduceDriver<Text, Text, Text, Text>();
		
		List<Text> valueList = new ArrayList<Text>();
		valueList.addAll(Arrays.asList(new Text("Cooper, Chris (I)"), new Text("Andrade, Jack"), new Text("Beauchene, Bill")));
		
		StringBuffer expectedList = new StringBuffer();
		expectedList.append("Cooper, Chris (I);Andrade, Jack;Beauchene, Bill");
		
		
		Text title = new Text("Me, Myself & Irene");
		reduceDriver.withReducer(new ShowActorsInMovie.ActorsListReducer())
		.withInput(title, valueList)
		.withOutput(title,new Text(expectedList.toString()))
		.runTest();
		
		System.out.println("expected output:" + reduceDriver.getExpectedOutputs());
	}
	

}
