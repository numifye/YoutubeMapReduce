package youtube; 

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRdriver extends Configured implements Tool {
    @SuppressWarnings("deprecation")
   public int run(String[] args) throws Exception {
        // TODO 1: configure  MR job
		Job job = new Job(getConf(), "youtube"); // "youtube" = string job name
		job.setJarByClass(MRdriver.class);
		job.setMapperClass(MRmapper.class);
		job.setReducerClass(MRreducer.class);
		job.setInputFormatClass(TextInputFormat.class); //files from plain txt files broken into lines
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        // TODO 2: setup input and output paths for MR job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // TODO 3: run  MR job syncronously with verbose output set to true
		return job.waitForCompletion(true) ? 0 : 1;
   }

   public static void main(String[] args) throws Exception { 
       if(args.length != 2) {
           System.err.println("usage: MRdriver <input-path> <output-path>");
           System.exit(1);
       }
       Configuration conf = new Configuration();
       System.exit(ToolRunner.run(conf, new MRdriver(), args));
   } 
}








/*
 hadoop jar <jar-file> youtube.MRdriver <input file> <output dir>

The output MUST match the following and be sorted in ascending order of category id's:
category_id: <id>
most views: video_id,thumbnail_link
most likes: video_id,thumbnail_link
most dislikes: video_id,thumbnail_link

For example: 

category_id: 1	
most views: ufaDurSCKOk,https://i.ytimg.com/vi/ufaDurSCKOk/default.jpg
most likes: 0R7MQwmbiQc,https://i.ytimg.com/vi/0R7MQwmbiQc/default.jpg
most dislikes: dt__kig8PVU,https://i.ytimg.com/vi/dt__kig8PVU/default.jpg
category_id: 10	
most views: MBdVXkSdhwU,https://i.ytimg.com/vi/MBdVXkSdhwU/default.jpg
most likes: MBdVXkSdhwU,https://i.ytimg.com/vi/MBdVXkSdhwU/default.jpg
most dislikes: 1NyMSWqIJDQ,https://i.ytimg.com/vi/1NyMSWqIJDQ/default.jpg
category_id: 15	
most views: evvVtqmvE5w,https://i.ytimg.com/vi/evvVtqmvE5w/default.jpg
most likes: evvVtqmvE5w,https://i.ytimg.com/vi/evvVtqmvE5w/default.jpg
most dislikes: U2CqZNd6rgM,https://i.ytimg.com/vi/U2CqZNd6rgM/default.jpg
*/
