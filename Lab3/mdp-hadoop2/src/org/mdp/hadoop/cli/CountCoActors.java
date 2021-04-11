package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Java class to run a remote Hadoop word count job.
 * 
 * Contains the main method, an inner Reducer class 
 * and an inner Mapper class.
 * 
 * @author Aidan
 */
public class CountCoActors {
	
	/**
	 * Use this with line.split(SPLIT_REGEX) to get fairly nice
	 * word splits.
	 */
	public static String SPLIT_REGEX = "\t";

	/**
	 * This is the Mapper Class. This sends key-value pairs to different machines
	 * based on the key.
	 * 
	 * Remember that the generic is Mapper<InputKey, InputValue, MapKey, MapValue>
	 * 
	 * InputKey we don't care about (a LongWritable will be passed as the input
	 * file offset, but we don't care; we can also set as Object)
	 * 
	 * InputKey will be Text: a line of the file
	 * 
	 * MapKey will be Text: a word from the file
	 * 
	 * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
	 * 
	 * @author Aidan
	 *
	 */
	public static class CountCoActorsMapper extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();

		/**
		 * Given the offset in bytes of a line (key) and a line (value),
		 * we will output (word,1) for each word in the line.
		 * 
		 * @throws InterruptedException 
		 * 
		 */
		@Override
		public void map(Object key, Text value, Context output)
				throws IOException, InterruptedException {
		 	// 'La Veneno', Cristina   La venganza de la Veneno        1997    null    VIDEO_MOVIE     null    1       null    FEMALE
			String line = value.toString();
			// ['La Veneno', Cristina, La venganza de la Veneno, 1997, null, VIDEO_MOVIE, null, 1, null, FEMALE]
			String[] element = line.split(SPLIT_REGEX);

			if (element[4].equals("THEATRICAL_MOVIE")) {
				String movie_name = element[1];
				String year = element[2];
				String movie_number = element[3];
				String llave = movie_name + "##" + year + "##" + movie_number;
				word.set(llave);
				output.write(word, new Text(element[0]));
			}
		}
	}


	/**
	 * This is the Reducer Class.
	 * 
	 * This collects sets of key-value pairs with the same key on one machine. 
	 * 
	 * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
	 * 
	 * MapKey will be Text: a word from the file
	 * 
	 * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
	 * 
	 * OutputKey will be Text: the same word
	 * 
	 * OutputValue will be IntWritable: the final count
	 * 
	 * @author Aidan
	 *
	 */
	public static class CountCoActorsReducer extends Reducer<Text, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/**
		 * Given a key (a word) and all values (partial counts) for 
		 * that key produced by the mapper, then we will sum the counts and
		 * output (word,sum)
		 * 
		 * @throws InterruptedException 
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {

			ArrayList<String> actores = new ArrayList<>();
			for (Text value: values) {
				actores.add(value.toString());
			}
			int a = 1;
			int b = 2;
			for (String actor: actores) {
				while (a < actores.size()) {
					String llave = actor + "##" + actores.get(a);
					a+=1;
					word.set(llave);
					output.write(word, one);
				}
				a = b;
				b+=1;
			}
		}
	}

	/**
	 * Main method that sets up and runs the job
	 * 
	 * @param args First argument is input, second is output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: "+CountCoActors.class.getName()+" <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];

		System.out.println("Leyendo Archivo " + inputLocation);

		Job job = Job.getInstance(new Configuration());

		FileInputFormat.setInputPaths(job, new Path(inputLocation));
		FileOutputFormat.setOutputPath(job, new Path(outputLocation));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.out.println("Seteando mapa");
		job.setMapperClass(CountCoActors.CountCoActorsMapper.class);
		System.out.println("Seteando Reducer");
		job.setReducerClass(CountCoActorsReducer.class);

		job.setJarByClass(CountCoActors.class);
		System.out.println("Ejecutando todo");
		job.waitForCompletion(true);
	}	
}
