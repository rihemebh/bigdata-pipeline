package tn.insat.pipeline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Function;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaSparkDataStocking {

    private static final Pattern COMMA = Pattern.compile(",");
    private static final Pattern SEMI_COMMA = Pattern.compile(";");
    private static final File csvOutputFile = new File("CSV_FILE.txt");
    private KafkaSparkDataStocking() {

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KafkaSparkDataStocking <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkDataStocking");
        // Creer le contexte avec une taille de batch de 2 secondes
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(5000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");

        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }
        // first solution
        /*
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) throws IOException {
                        try(FileWriter fr = new FileWriter(csvOutputFile, true);PrintWriter pw = new PrintWriter(fr);){
                            pw.println(x);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return Arrays.asList(COMMA.split(x)).iterator();
                    }
                });*/
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) throws IOException {
                        try (FileWriter fr = new FileWriter(csvOutputFile, true); PrintWriter pw = new PrintWriter(fr);) {
                            String[] lines = SEMI_COMMA.split(x);
                            for(String line:lines)
                                if(!Objects.equals(line, ""))
                                    pw.println(line);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return Arrays.asList(COMMA.split(x)).iterator();
                    }
                });

        words .print();

        jssc.start();
        jssc.awaitTermination();

    }
}
