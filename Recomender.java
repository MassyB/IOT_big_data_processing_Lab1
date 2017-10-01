import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;
import java.lang.String;


public class Recomender {

  //Mapper class
  public static class RecomenderMapper
    extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

          String line = value.toString();
          String[] items = line.split(",");

          for(String firstItem: items){
            for(String secondItem: items){
              // don't consider the item with itself
              if(! firstItem.equals(secondItem)){
                context.write(new Text(firstItem), new Text(secondItem));
              }

            }
          }
        }
  }
  //end Mapper class

  //Reducer class
  public static class RecomenderReducer
    extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {

            HashSet<String> items = new HashSet<String>();
            for(Text item: values){
              // construct the items for the key
              items.add(item.toString());
            }
            // construct the string to store
            String suggestions = join(items);
            context.write(key, new Text(suggestions));
        }

        private String join(Iterable<String> items){
            String result = "";
            for(String item: items){
                result = result +"," + item;
            }
            return result;
        }
  }
  //end Reducer class

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Recomender System");
    job.setJarByClass(Recomender.class);

    job.setMapperClass(RecomenderMapper.class);
    job.setReducerClass(RecomenderReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);


  }
}
