package youtube;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MRreducer  extends Reducer <Text,Text,Text,Text> {
    public static String IFS=",";
    public static String OFS=",";
    public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
        /** values consist of:
         * vidId
         * link
         * views
         * likes
         * dislikes
        */
    	// TODO 1: initialize variables
    	Text tmpVid = null, tmpLink = null;
    	//views
    	long maxViews = Long.MIN_VALUE;
    	long tmpViews = 0L;
    	Text viewsVid = null, viewsLink = null;
    	//likes
    	long maxLikes = Long.MIN_VALUE;
    	long tmpLikes = 0L;
    	Text likesVid = null, likesLink = null;
    	//dislikes
    	long maxDis = Long.MIN_VALUE;
    	long tmpDis = 0L;
    	Text disVid = null, disLink = null;
    	
    	// TODO 2: loop through values to find most viewed, liked, and disliked
    	String compositeString;
    	String[] compositeStringArray;
    	for (Text value: values) { //for every Text value w/ this key (cat_id)
    		compositeString = value.toString();
    		compositeStringArray = compositeString.split(IFS);
    		tmpVid = new Text(compositeStringArray[0]);
    		tmpLink = new Text(compositeStringArray[1]);
    		tmpViews = new Long(compositeStringArray[2]).longValue();
    		tmpLikes = new Long(compositeStringArray[3]).longValue();
    		tmpDis = new Long(compositeStringArray[4]).longValue();
    		if(tmpViews > maxViews){
    			maxViews = tmpViews;
    			viewsVid = tmpVid;
    			viewsLink = tmpLink;
    		}
    		if(tmpLikes > maxLikes){
    			maxLikes = tmpLikes;
    			likesVid = tmpVid;
    			likesLink = tmpLink;
    		}
    		if(tmpDis > maxDis){
    			maxDis = tmpDis;
    			disVid = tmpVid;
    			disLink = tmpLink;
    		}
    	}
    	
    	// TODO 3: write the key-value pair to the context exactly as defined in lab write-up
    	Text keyText1 = new Text("category_id: ");
    	context.write(keyText1, key);
    	Text keyText2 = new Text("most views: ");
    	context.write(keyText2, new Text(viewsVid + "," + viewsLink));
    	Text keyText3 = new Text("most likes: ");
    	context.write(keyText3, new Text(likesVid + "," + likesLink));
    	Text keyText4 = new Text("most dislikes: ");
    	context.write(keyText4, new Text(disVid + "," +disLink));
    	
   }
}

