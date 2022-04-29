import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class ColumnRefactorTask {

    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new ColumnRefactorTask().run(args[0], args[1]);
    }


    public void run(String inputFilePath, String outputDir) {

        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(ColumnRefactorTask.class.getName())
        .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        List<String> rows = textFile.collect();
        List<String> newRows = new ArrayList<>();
        for (String name: rows
             ) {
            String [] a = name.split(",");
            if(a[a.length - 1].endsWith("\""))
            {
                a[a.length-2] = a[a.length-2]+'.'+a[a.length-1];
                a[a.length-1] = null;
                StringJoiner sj = new StringJoiner(",");
                for(String s:a) {
                    if(s != null)
                    sj.add(s);
                }
                newRows.add(String.valueOf(sj));
            }
            else if(a[a.length - 1].equals("C")) {
                a[a.length - 1] = "0";
                StringJoiner sj = new StringJoiner(",");
                for(String s:a) sj.add(s);
                newRows.add(String.valueOf(sj));
            }else{
                newRows.add(name);
            }

        }
        JavaRDD<String> rdd = sc.parallelize(newRows);

        rdd.saveAsTextFile(outputDir);
    }
}