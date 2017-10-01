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
import java.lang.*;


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

        private HashMap<String,Integer> itemsCount;

        @Override
        public void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {

            // construct the count for each item
            itemsCount = new HashMap<String,Integer>();
            for(Text item: values){

              if(itemsCount.containsKey(item.toString()))
                itemsCount.put(item.toString(),itemsCount.get(item.toString()).intValue()+1);
              else
                itemsCount.put(item.toString(),new Integer(1));
            }
            // get an array of items
            Set<String> itemSet = itemsCount.keySet();
            String[] items = new String[itemSet.size()];
            int i=0;
            for(String item: itemSet){
              items[i] = item;
              i++;
            }
            // sort the items
            sortDecreasingly(items);
            // construct the output of the reduce
            String suggestions = join(items);
            // write the reduce output value
            context.write(key, new Text(suggestions));
        }

        private void sortDecreasingly(String[] items){

          Arrays.sort(items, new Comparator<String>() {
            public int compare(String item1, String item2) {
                // sort in a decreasing order
                if (itemsCount.get(item1) > itemsCount.get(item2))
                   return -1;
                else if(itemsCount.get(item1) < itemsCount.get(item2))
                   return +1;
                else
                   return 0;
              }
          });
        }

        private String join(String[] items){
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
