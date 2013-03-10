Picasa Equalize with Hadoop
===========================

* Main function gets image URLs from Picasa API for public images 
* Mappers fetch images and save them to HDFS. 
* Reducers perform histogram equalization on the saved images and save the results to HDFS.
	* Can be done in combiner, since outputs are all independent of each other

##Build

	mvn package

##Run

Run the jar file with Hadoop. Program arguments:

1. Path to the HDFS directory to save the unequalized images
2. Path to the HDFS directory to save the equalized images
3. Number of images to fetch (actual result may be less due to 404s and animated GIFs)
4. Search term (Optional. If not specified, use featured images.)

Example:

	hadoop jar target/PicasaEqualize-1.0.jar edu.cooper.ece460.picasa.PicasaEqualize picasa_in picasa_out 5 landscape
	hadoop fs -copyToLocal picasa_in/* /path/to/unequalized/images
	hadoop fs -copyToLocal picasa_out/* /path/to/equalized/images

Note that the equalized images directory must not already exist. It can be removed using:

	hadoop fs -rmr picasa_out 

