package hybirdApproach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HybridMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
	private Map<Pair, Integer> map = new HashMap<Pair, Integer>();

	// Create and add pairs to the map. With in-mapper combining.
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] allTerms = value.toString().split("\\s+");

		for (int i = 0; i < allTerms.length; i++) {
			String term = allTerms[i];
			for (int j = i + 1; j < allTerms.length; j++) {
				if (term.equals(allTerms[j]))
					break;

				// Create a pair
				Pair keyTerm = new Pair(term, allTerms[j]);

				// Add pair to the map
				if (!map.containsKey(keyTerm)) {
					map.put(keyTerm, 1);
				} else {
					map.put(keyTerm, map.get(keyTerm) + 1);
				}
			}
		}
	}

	// Writes all the pairs in map to the context
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		Iterator<Entry<Pair, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Pair, Integer> entry = iterator.next();
			context.write(entry.getKey(), new IntWritable(entry.getValue()));
			iterator.remove();
		}
	}
}