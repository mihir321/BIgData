package pairsApproach;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	private Text current = new Text();
	int totalCount = 0;

	// Sums up all the actual pairs and the count pairs. Calculate relative
	// frequencies and writes the result to the context.
	@Override
	protected void reduce(Pair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		// Check if it is a star pair
		if (key.getSecond().toString().equals("*")) {
			if (key.getFirst().equals(current)) {
				totalCount += sumAllValues(values);
			} else {
				current.set(key.getFirst());
				totalCount = 0;
				totalCount += sumAllValues(values);
			}
		} else {
			// if not star pair write the pair with relative freq to the
			// context
			int sum = sumAllValues(values);
			double value = (double) sum / (double) totalCount;
			context.write(key, new DoubleWritable(value));
		}
	}

	// returns the sum of all the values
	private int sumAllValues(Iterable<IntWritable> values) {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		return count;
	}
}