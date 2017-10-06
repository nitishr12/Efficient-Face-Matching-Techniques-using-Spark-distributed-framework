# linearcompare.py
# Running the  linear comparison of N images in the dataset with input image 
# Standalone Python/Spark program to perform linear comparison of N images with input image and return top 10 matches
# 
# Loads the input image from argument 1
# Loads the imageset folder from the argument 2  
# 
# 
# ITCS 6190 - Cloud Computing - Final Project
# Lakshminarayana Achar Belman Ramachandra
# 800959710
# lbelmanr@uncc.edu
# TODO: Write this.
# 
# 
# 
#
# Usage: spark-submit linearcompare.py <pathTo-input file> <pathTo-imageset> 
# Example usage: spark-submit linearcompare.py obama.jpg minten200/
#
#
import sys
import face_recognition as fc
import numpy as np
import copy
import time

from pyspark import SparkContext

#Calculate the encoding for each Image name using face_recognition lib and return the encoding
def key_face_encoding(l):
	print "Inside face_encoding"
	print l
	image = fc.load_image_file(l)
	face_encoding = fc.face_encodings(image)
	face_matrix = np.asmatrix(face_encoding)
	print "face-matrix"
	print face_matrix
	return face_matrix
#Calculate the facedistance value for image with the input image and return the imagename,facedistance
def face_distance(A,l):
	print "Inside face distance"
	print "l[0]"
	print l[0]
	print "l[1]"
	print l[1]
	lmatrix = np.asmatrix(l[1])
	facedistance=1.0
	shapeA=np.shape(A)[1]
	shapeL=np.shape(lmatrix) [1]
	print shapeA
	print shapeL
	if shapeA==shapeL:
		facedistance=np.linalg.norm(lmatrix-A) 
	return (l[0],facedistance)
	


if __name__ == "__main__":
  if len(sys.argv) !=3:
    print >> sys.stderr, "linearcompare.py <pathTo-input file> <pathTo-imageset>"
    exit(-1)
  stime = time.time()
  sc = SparkContext(appName="LinearFaceDistanceComparison")

 
  inputFile = sc.wholeTextFiles(sys.argv[1])
  imageset = images = sc.wholeTextFiles(sys.argv[2])
  strpath = str(sys.argv[2])	

  # Generate the encoding for the Input image
  A = np.asmatrix(key_face_encoding(sys.argv[1]))
 



  #Generate the face encoding  for each image and calculate the face distance and sort the list by face distance value
  B_RDD = imageset.map(lambda l:l).map(lambda l1: strpath+(str(l1).split(',')[0].split(strpath)[1] [:-1])).map(lambda l2: (l2,key_face_encoding(l2))).filter(lambda l: np.shape(l[1])[1]==128).map(lambda l3: face_distance(A,l3)).map(lambda (a,b):(b,a)).sortByKey(ascending=True).map(lambda (a,b):(b,a))
  # take first 10 results
  B=B_RDD.take(10)
  # save output in a file called out_compare
  B_RDD.coalesce(1).saveAsTextFile("out_compare")  

  print "A Val " 
  print A

  print "A shape"
  print np.shape(A)

 	

  print "B val"
  print np.array(B).tolist()

  for dist in B:
	print dist 	

 
  print "Time :"
  print time.time()-stime
 
	
 	

  sc.stop()
