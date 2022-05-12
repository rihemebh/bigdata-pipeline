package tn.insat.tp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Objects;

public class HbaseSparkProcess {

    public void run(String name, String year) {
        Configuration config = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        config.set(TableInputFormat.INPUT_TABLE,"Enterprise-survey");
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        if(Objects.equals(year, ""))
            System.out.println("\n\n*********** Stats for "+name+" ***********\n\n");
        else
            System.out.println("\n\n*********** Stats for "+name+" -- Year : "+year+" ***********\n\n");
        JavaRDD<String> javaRDD = hBaseRDD.map((Function<Tuple2<ImmutableBytesWritable, Result>, String>) tuple -> {
            Result result = tuple._2;
            String industryName = Bytes.toString(result.getValue(Bytes.toBytes("EnterpriseData"), Bytes.toBytes("Industry_name")));
            String value="1";
            if(Objects.equals(industryName, name)){
                String surveyVariable = Bytes.toString(result.getValue(Bytes.toBytes("SurveyVariable"), Bytes.toBytes("Variable_name")));
                value = Bytes.toString(result.getValue(Bytes.toBytes("SurveyVariable"), Bytes.toBytes("Value")));
                String surveyYear = Bytes.toString(result.getValue(Bytes.toBytes("EnterpriseData"), Bytes.toBytes("Year")));
                if(Objects.equals(year, "")){
                    System.out.println("Year: "+surveyYear+" ---  Variable: "+surveyVariable+" --- Value: "+value);
                }else if (Objects.equals(year,surveyYear )){
                    System.out.println("Variable: "+surveyVariable+" --- Value: "+value);
                }
            }
            return value;
        });

        System.out.println("\n\n RDDs : "+javaRDD.count()+"\n\n");
    }

    public static void main(String[] args) throws InterruptedException {
        if(args.length == 2)
            new HbaseSparkProcess().run(args[0],args[1]);
        else if (args.length == 1)
            new HbaseSparkProcess().run(args[0],"");
        else
            System.out.println("Please provide enterprise name");
    }

}