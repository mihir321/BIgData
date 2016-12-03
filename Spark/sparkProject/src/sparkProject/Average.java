package sparkProject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Map;
import java.util.regex.Pattern;


public class Average {
	private static final Pattern SPACES = Pattern.compile(" ");
	static Map<String, Object> KeyCount;
          
      private static final PairFunction<String, String, Integer> WORDS_MAPPER = 
      		new PairFunction<String, String, Integer>() {
				@Override
        public Tuple2<String, Integer> call(String s) {
          String[] parts = SPACES.split(s);
        
          return new Tuple2<String, Integer>(parts[0], Integer.parseInt(parts[1]));
        }
      };      

      private static final PairFunction<Tuple2<String, Integer>, String, Integer> WORDS_MAPPER2 = 
      		new PairFunction<Tuple2<String, Integer>, String, Integer>() {
				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple)
						throws Exception {
		
					return new Tuple2<String, Integer>(tuple._1, tuple._2/Integer.parseInt(KeyCount.get(tuple._1).toString()));
				}
      };
      
          
  private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
      new Function2<Integer, Integer, Integer>() {
        /**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
        public Integer call(Integer a, Integer b) throws Exception {
          return a + b;
        }
      };

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }

    SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
    @SuppressWarnings("resource")
		JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> file = context.textFile(args[0]);
   
    JavaPairRDD<String, Integer> pairs = file.mapToPair(WORDS_MAPPER);
    
    KeyCount = pairs.countByKey();
    
    JavaPairRDD<String, Integer> pairs2 = pairs.mapToPair(WORDS_MAPPER2);
    JavaPairRDD<String, Integer> counter = pairs2.reduceByKey(WORDS_REDUCER);
    counter.saveAsTextFile(args[1]);
  }
}