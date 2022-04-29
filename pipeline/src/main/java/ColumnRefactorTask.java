import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class ColumnRefactorTask {

    public String joinRow(String[] tab){
        StringJoiner sj = new StringJoiner(",");
        for(String s:tab) {
            if(s != null)
                sj.add(s);
        }
        return String.valueOf(sj);
    }


    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new ColumnRefactorTask().run(args[0], args[1]);
    }


    public void run(String inputFilePath, String outputDir) {

        String master = "local[*]";
        /**
         * Don't forget to remove the setMaster config when you want to try it in the clusterZ
         */
        SparkConf conf = new SparkConf()
                .setAppName(ColumnRefactorTask.class.getName())
        .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        List<String> rows = textFile.collect();
        List<String> newRows = new ArrayList<>();
        for (String row: rows
             ) {
            String [] a = row.split(",");
            if(a[a.length - 1].endsWith("\""))
            {
                a[a.length-2] = a[a.length-2]+'.'+a[a.length-1];
                a[a.length-1] = null;

                newRows.add(joinRow(a));
            }
            else if(a[a.length - 1].equals("C")) {
                a[a.length - 1] = "0";

                newRows.add(joinRow(a));
            }else{
                newRows.add(row);
            }

        }
        JavaRDD<String> rdd = sc.parallelize(newRows);

        rdd.saveAsTextFile(outputDir);
    }
}