/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
public class SparkCovid19_1 {


    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Output file path on distributed file system
     */
    public static void main(String[] args){
        long start = System.currentTimeMillis();
        String input = "hdfs://localhost:9000"+args[0];
        String output = "hdfs://localhost:9000"+args[3];
        try{
            Date startDate = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
            Date endDate = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
            Date availableStart = new SimpleDateFormat("yyyy-MM-dd").parse("2019-12-31");
            Date availableEnd = new SimpleDateFormat("yyyy-MM-dd").parse("2020-04-08");
            if(startDate.compareTo(availableStart) < 0 || startDate.compareTo(availableEnd) > 0){
                System.out.println("Start Date should be between 2019-12-31 and 2020-04-08 inclusive.");
                return;
            }
            if(endDate.compareTo(availableStart) < 0 || endDate.compareTo(availableEnd) > 0){
                System.out.println("End Date should be between 2019-12-31 and 2020-04-08 inclusive.");
                return;
            }
            if(startDate.compareTo(endDate) > 0){
                System.out.println("Start Date should be less than or equal to End Date.");
                return;
            }
        }catch(Exception e){
            System.out.println("Invalid Date format ... Dates should be in the yyyy-mm-dd format only.");
            return;
        }
        System.out.println("All valid so far in input");
        /* essential to run any spark code */
        SparkConf conf = new SparkConf().setAppName("SparkCovid19_1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("All valid so far and got Spark Context");
        /* load input data to RDD */
        JavaRDD<String> dataRDD = sc.textFile(input);

        JavaPairRDD<String, Integer> counts =
                dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
                    public Iterator<Tuple2<String, Integer>> call(String value){
                        String[] lines = value.split("\n");
                        List<Tuple2<String, Integer>> retWords = new ArrayList<Tuple2<String, Integer>>();
                        try{
                            Date startDate = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
                            Date endDate   = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]);
                            for(String line: lines){
                                String[] tokens = line.split(",");
                                if(!tokens[2].equals("new_cases")){
                                    Date date = new SimpleDateFormat("yyyy-MM-dd").parse(tokens[0]);
                                    if(date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0){
                                        retWords.add(new Tuple2<String, Integer>(tokens[1],Integer.parseInt(tokens[3])));
                                    }
                                }
                            }
                        }catch(Exception e){
                            System.out.println("Exception XXX");
                        }

                        return retWords.iterator();
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>(){
                    public Integer call(Integer x, Integer y){
                        return x+y;
                    }
                });

        counts.saveAsTextFile(output);
        long end = System.currentTimeMillis();
        System.out.println("Total time take : "+(end-start)+" ms");
    }
}