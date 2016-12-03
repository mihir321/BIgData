package hybirdApproach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Text> {

	private long total = 0;
	private String previousKeyFirst = null;
	private Map<String, Integer> map = new TreeMap<String, Integer>();

	// Sums up all the pairs by creating a stripe from them, creates the output
	// string with relative frequencies and writes it to the context.
	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		String currentKeyFirst = key.getFirst().toString();
		String keySecond = key.getSecond().toString();

		// if first key changes, write the created stripe to the context
		if (previousKeyFirst != null
				&& !previousKeyFirst.equals(currentKeyFirst)) {

			WriteToContext(context);
			total = 0;
			previousKeyFirst = currentKeyFirst;
			map = new HashMap<String, Integer>();
		}

		float sum = 0;
		// Add the pairs to a map and create stripe
		for (IntWritable val : values) {
			Integer oldValue = map.get(keySecond);

			if (oldValue != null) {
				map.put(keySecond, val.get() + oldValue);
			} else {
				map.put(keySecond, val.get());
			}

			sum += val.get();
		}

		previousKeyFirst = currentKeyFirst;
		total += sum;
	}

	// Writes the last stripe to the context
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		WriteToContext(context);
		super.cleanup(context);
	}

	// Creates an output string for the stripe
	public static String convertMapDataToString(Map<String, Float> map) {
		if (map == null) {
			return "";
		}

		StringBuilder stringBuilder = new StringBuilder();
		String finalStringToReturn = "";

		Iterator<Entry<String, Float>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Float> pairs = (Map.Entry<String, Float>) iterator
					.next();
			stringBuilder.append(String.format("(%s,%s)", pairs.getKey(),
					pairs.getValue()));
			stringBuilder.append(" ");
		}

		if (stringBuilder.length() > 0) {
			finalStringToReturn = stringBuilder.substring(0,
					stringBuilder.length() - 1);
		}
		return finalStringToReturn;
	}

	// Writes the stripe to the context
	private void WriteToContext(Context context) throws IOException,
			InterruptedException {
		Iterator<Entry<String, Integer>> it = map.entrySet().iterator();
		Map<String, Float> pairsContainingRelativeFreq = new TreeMap<String, Float>();

		// calculate relative frequencies and add to the map
		while (it.hasNext()) {
			Map.Entry<String, Integer> neighborAndItsRelFreqPair = (Map.Entry<String, Integer>) it
					.next();
			pairsContainingRelativeFreq.put(neighborAndItsRelFreqPair.getKey(),
					(float) neighborAndItsRelFreqPair.getValue() / total);
		}

		String output = convertMapDataToString(pairsContainingRelativeFreq);
		context.write(new Text(previousKeyFirst), new Text(output));
	}
}
