package stripesApproach;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
	private MapWritable sumOfAllStrips = new MapWritable();

	// Sums up all the stripes for a given key, creates the output string with
	// relative frequencies and writes it to the context.
	@Override
	protected void reduce(Text key, Iterable<MapWritable> values,
			Context context) throws IOException, InterruptedException {

		sumOfAllStrips.clear();

		// sums up all strips into a single strip
		for (MapWritable value : values) {
			addStripToResultantStrip(value);
		}

		int totalCount = 0;
		// get the total count which is needed to calculate relative frequencies
		for (Entry<Writable, Writable> entry : sumOfAllStrips.entrySet()) {
			totalCount += Integer.parseInt(entry.getValue().toString());
		}

		DecimalFormat decimalFormat = new DecimalFormat("0.0000");
		StringBuilder stringBuilder = new StringBuilder();

		// Creating an output string for the reducer with relative frequencies.
		for (Entry<Writable, Writable> entry : sumOfAllStrips.entrySet()) {
			stringBuilder
					.append("(")
					.append(entry.getKey().toString())
					.append(",")
					.append(decimalFormat.format(Integer.parseInt(entry
							.getValue().toString()) / (double) totalCount))
					.append(") ");
		}

		// write stripe to context
		context.write(key, new Text("[ " + stringBuilder.toString() + "]"));
	}

	// Add the strip to the resultant strip .
	private void addStripToResultantStrip(MapWritable mapWritable) {
		Set<Writable> allKeys = mapWritable.keySet();
		for (Writable key : allKeys) {
			IntWritable keyCountInCurrentStrip = (IntWritable) mapWritable
					.get(key);
			if (sumOfAllStrips.containsKey(key)) {
				IntWritable keyCountInResultantStrip = (IntWritable) sumOfAllStrips
						.get(key);
				keyCountInResultantStrip.set(keyCountInResultantStrip.get()
						+ keyCountInCurrentStrip.get());
			} else {
				sumOfAllStrips.put(key, keyCountInCurrentStrip);
			}
		}
	}
}
