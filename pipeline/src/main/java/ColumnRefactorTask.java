import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;

import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

public class ColumnRefactorTask {
    public final static String TOPIC_NAME = "Bigdata_pipeline";

    public String joinRow(String[] tab) {
        StringJoiner sj = new StringJoiner(",");
        for (String s : tab) {
            if (s != null)
                sj.add(s);
        }
        return String.valueOf(sj);
    }


    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file as parameters.");
        new ColumnRefactorTask().run(args[0]);
    }


    public void run(String inputFilePath) {

        SparkConf conf = new SparkConf()
                .setAppName(ColumnRefactorTask.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        List<String> rows = textFile.collect();
        List<String> newRows = new ArrayList<>();
        for (String row : rows
        ) {
            String[] a = row.split(",");
            if (a[a.length - 1].endsWith("\"")) {
                a[a.length - 2] = a[a.length - 2] + '.' + a[a.length - 1];
                a[a.length - 1] = null;

                newRows.add(joinRow(a));
            } else if (a[a.length - 1].equals("C")) {
                a[a.length - 1] = "0";

                newRows.add(joinRow(a));
            } else {
                newRows.add(row);
            }

        }

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.put("acks", "all");

        props.put("retries", 0);

        props.put("batch.size", 16384);

        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        for (String newRow : newRows) {
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME,
                    newRow));

        }
        System.out.println("Dataset sent seccussfully");
        producer.close();
    }
}