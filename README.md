Picasa Equalize with Hadoop
===========================

* Mappers fetch images from Picasa API for public images and save them to HDFS. 
* Reducers perform histogram equalization on the saved images and save the results to HDFS.

Note: Does not work properly if the fetched images happen to be animated GIFs.

##Build

	mvn package

##Run

Run the jar file with Hadoop. Program arguments:

1. Path to the HDFS directory to save the unequalized images
2. Path to the HDFS directory to save the equalized images
3. Number of API calls to make (i.e. Number of map tasks)
4. Number of images per API call
5. Search term

Example:

	hadoop jar target/PicasaEqualize-1.0.jar edu.cooper.ece460.picasa.PicasaEqualize picasa_in picasa_out 2 5 animals
	hadoop fs -copyToLocal picasa_in/* /path/to/unequalized/images
	hadoop fs -copyToLocal picasa_out/* /path/to/equalized/images

Note that the equalized images directory must not already exist. It can be removed using:

	hadoop fs -rmr picasa_out 

