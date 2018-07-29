# YoutubeMapReduce
Calculated basic statistics for the YouTube data set (USvideos.csv only) from Kaggle using a single Hadoop MapReduce program.


Implemented the MapReduce code that provides the following — all in a single output file — per category stats from USvideos.csv:

	-most viewed video
  
	-most liked video
  
	-most disliked video
  

Code will be compiled into jar file and ran as follows:

	-hadoop jar <jar-file> youtube.MRdriver <input file> <output dir>


Required output (ascending order of category id’s):

category_id: <id>

most views: video_id,thumbnail_link

most likes: video_id,thumbnail_link

most dislikes: video_id,thumbnail_link
