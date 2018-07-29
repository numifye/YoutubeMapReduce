package youtube;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class MRmapper  extends Mapper <LongWritable,Text,Text,Text> {
    static String IFS=",";
    static String OFS=",";
    static int NF=11;
    
    public void map(LongWritable key, Text value, Context context) 
                    throws IOException, InterruptedException {
        
        /** USvideos.csv
        video_id
        title
        channel_title
        category_id
        tags
        views
        likes
        dislikes
        comment_total
        thumbnail_link
        date
        */
        
        // TODO 1: remove schema line
    	//the key is the bite offset into the file, so schema line key = 0
    	if(key.get() == 0){
    		return;
    	}
    	// TODO 2: convert value to string
    	String lineString = value.toString();
    	String[] lineArray = lineString.split(IFS);
        // TODO 3: check num fields, return if bad
        if (lineArray.length != NF){
        	return;
        }
        // TODO 4: pull out fields of interest:
        String catId = lineArray[3];
        String vidId = lineArray[0];
        String views = lineArray[5];
        String likes = lineArray[6];
        String dislikes = lineArray[7];
        String link = lineArray[9];
        // TODO 5: construct key and composite value
        // TODO 6: write key value pair to context
        //System.out.print(catId + " " + views + " " + likes);
        context.write(new Text(catId), new Text(vidId + OFS + link + OFS + views + OFS + likes + OFS + dislikes));
        
    }
}
