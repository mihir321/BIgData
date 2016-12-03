package hybirdApproach;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Writable, WritableComparable<Pair> {

	private Text firstElement;
	private Text secondElement;

	// Pair Constructor that takes two texts
	public Pair(Text first, Text second) {
		this.firstElement = first;
		this.secondElement = second;
	}

	// Pair Constructor that takes two Strings
	public Pair(String first, String second) {
		this(new Text(first), new Text(second));
	}

	public Pair() {
		this.firstElement = new Text();
		this.secondElement = new Text();
	}

	// Comparator method for Pair
	@Override
	public int compareTo(Pair object2) {

		// Compare first element
		int result = this.firstElement.compareTo(object2.getFirst());
		if (result != 0) {
			return result;
		}

		// Compare first element
		return this.secondElement.compareTo(object2.getSecond());
	}

	// Writes data to the output
	@Override
	public void write(DataOutput out) throws IOException {
		firstElement.write(out);
		secondElement.write(out);
	}

	// Reads data
	@Override
	public void readFields(DataInput in) throws IOException {
		firstElement.readFields(in);
		secondElement.readFields(in);
	}

	// Convert pair to string
	@Override
	public String toString() {
		return "(" + firstElement + "," + secondElement + ")";
	}

	// Check the equality of a pair
	@Override
	public boolean equals(Object object2) {
		// Check if same objects
		if (this == object2)
			return true;

		// Check the class type
		if (object2 == null || this.getClass() != object2.getClass())
			return false;

		Pair objectPair = (Pair) object2;

		// Check equality of first element
		boolean isEqualFirst = (firstElement != null ? firstElement
				.equals(objectPair.firstElement)
				: objectPair.firstElement == null);

		// Check equality of second element
		boolean isEqualSecond = (secondElement != null ? secondElement
				.equals(objectPair.secondElement)
				: objectPair.secondElement == null);

		if (isEqualFirst && isEqualSecond)
			return true;

		return false;
	}

	// Returns the unique hash code for a pair
	@Override
	public int hashCode() {
		int result = firstElement != null ? firstElement.hashCode() : 0;

		
		result = 57 * result
				+ (secondElement != null ? secondElement.hashCode() : 0);
		return result;
	}

	// Returns first text from a pair
	public Text getFirst() {
		return firstElement;
	}

	// Returns second text from a pair
	public Text getSecond() {
		return secondElement;
	}
}
