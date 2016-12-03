package stripesApproach;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {
	private MapWritable strip = new MapWritable();
	private Text keyTerm = new Text();

	// Create and write stripes to the context.
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] allTerms = value.toString().split("\\s+");

		for (int i = 0; i < allTerms.length; i++) {
			keyTerm.set(allTerms[i]);

			// Clear the old strip
			strip.clear();

			// Create strip for the current keyterm.
			for (int j = i + 1; j < allTerms.length; j++) {
				if (keyTerm.toString().equals(allTerms[j]))
					break;

				Text neighborTerm = new Text(allTerms[j]);
				if (strip.containsKey(neighborTerm)) {
					IntWritable count = (IntWritable) strip.get(neighborTerm);
					count.set(count.get() + 1);
				} else {
					strip.put(neighborTerm, new IntWritable(1));
				}
			}

			// write the strip to the context if map is not empty
			if (!strip.isEmpty())
				context.write(new Text(keyTerm), strip);
		}
	}

}
