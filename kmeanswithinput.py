# kmeanswithinput.py
# Standalone Python/Spark program to perform search using clusters formed from k means algorithm
# Fetches the cluster centers, its images and runs the input file with the cluster centers to find the closest cluster
# Run the input file with the images in the closest cluster to return the top 10 images. We retrieve three best clusters for getting better results
# ITCS 6190 - Cloud Computing - Final Project
# Nitish Rangarajan
# 800963268
# nrangara@uncc.edu
# Usage: spark-submit kmeanswithinput.py <inputimage>
# Example usage:  spark-submit kmeans.py minten200/gates_1.jpg

import sys
import face_recognition as fc
import numpy as np
import copy
import time
import random

from pyspark import SparkContext

def key_face_encoding(l):
	# This returns the face encoding of the given image in l
	print "Inside face_encoding"
	#print l
	image = fc.load_image_file(l)
	face_encoding = fc.face_encodings(image)
	face_matrix = np.asmatrix(face_encoding)
	#print "face-matrix"
	#print face_matrix
	return face_matrix

if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkContext(appName="LinearFaceDistanceComparison")
  #Read the input file
  inputFile = sc.wholeTextFiles(sys.argv[1])
  A = np.asmatrix(key_face_encoding(sys.argv[1]))
  #Get the start time
  start_time=time.time()
  res_RDD=sc.pickleFile("Results")
  res=res_RDD.collect()
  #Retrieve the cluster centers
  clus_RDD=sc.pickleFile("Cluster")
  clusCenters=clus_RDD.collect()
  clusterCenters=np.array(clusCenters)
  imageCount=10
  i=0
  facedistance=[1.0 for x in range(len(clusterCenters))]

  #Find the best cluster for the image
  for dist in clusterCenters:
  	dist=np.asmatrix(dist)
	shapeA=np.shape(A)
	shapeL=np.shape(dist)
	if shapeA==shapeL:
		facedistance[i]=np.linalg.norm(A-dist)
	i=i+1

  #Find the cluster with the least face distance
  for index in range(len(facedistance)):
		if facedistance[index]==min(facedistance):	
			minimum=index
  print "The input image "+str(sys.argv[1])+" matches with the cluster "+str(minimum+1)
  #Get all the images from that cluster
  finalResult=res_RDD.map(lambda l:l).filter(lambda l1: l1[0]==minimum+1).map(lambda l: l[1])
  print finalResult.collect()[0]
  finalResult=sc.parallelize(finalResult.collect()[0])
  print finalResult
  resultCount=len(finalResult.collect())
  print "The result count is"+str(resultCount)

  #Get all the images from the next closest cluster
  minimum1_val=np.partition(facedistance, 2)[2]
  for index in range(len(facedistance)):
		if facedistance[index]==minimum1_val:	
			minimum1=index
  print "Second "+str(minimum1)
  finalResult1=res_RDD.map(lambda l:l).filter(lambda l1: l1[0]==minimum1+1).map(lambda l: l[1])
  print finalResult1.collect()
  finalResult1=sc.parallelize(finalResult1.collect()[0])
  print finalResult1
  resultCount=len(finalResult1.collect())
  print "The result count is"+str(resultCount)
  finalResult2=finalResult1.union(finalResult)

  #Get all the images from the next closest cluster
  minimum2_val=np.partition(facedistance, 3)[3]
  for index in range(len(facedistance)):
		if facedistance[index]==minimum2_val:	
			minimum2=index
  print "Third "+str(minimum2)
  finalResult1=res_RDD.map(lambda l:l).filter(lambda l1: l1[0]==minimum2+1).map(lambda l: l[1])
  print finalResult1.collect()
  finalResult1=sc.parallelize(finalResult1.collect()[0])
  print finalResult1

  print "The result count is"+str(resultCount)
  finalResult2=finalResult2.union(finalResult1)
  resultCount=len(finalResult2.collect())
  i=4
  if(resultCount>=10):
	#Get the face encoding for the images in the matching cluster, get face distance with the input and sort
	finalDistance_RDD=finalResult2.map(lambda l:(l,key_face_encoding(l))).map(lambda l:(l[0],np.linalg.norm(A-l[1]))).map(lambda (a,b):(b,a)).sortByKey(ascending=True).map(lambda (a,b):(b,a))
	#Get the top 10 results
	finalDistance=finalDistance_RDD.take(10)
	print finalDistance
	#Save the RDD to a text file
  	finalDistance_RDD.coalesce(1).saveAsTextFile("out_kmeans_compare")

  else:
	print "Count is less than 10"
	while(imageCount>=0):
		#Get the face encoding for the images in the matching cluster, get face distance with the input and sort
		finalDistance_RDD=finalResult2.map(lambda l:(l,key_face_encoding(l))).map(lambda l:(l[0],np.linalg.norm(A-l[1]))).map(lambda (a,b):(b,a)).sortByKey(ascending=True).map(lambda (a,b):(b,a)).take(resultCount)
		if i>4:
			#Union the RDDs with the older
			finalDistance_RDD=finalDistance_RDD.union(finalDistanceRDDs)
		imageCount=imageCount-resultCount
		#Get the ith best cluster and do the same process for getting the top 10 results
		minimum3=np.partition(facedistance, i)[i]
		print "The input image "+str(sys.argv[1])+" matches with the cluster "+str(minimum3+1)
		finalResult2=res_RDD.map(lambda l:l).filter(lambda l1: l1[0]==minimum3+1).map(lambda l: l[1])
		finalResult2=sc.parallelize(finalResult2.collect()[0])
  		resultCount=len(finalResult2.collect()[0])
		if resultCount>=10:
			finalDistance_RDD1=finalResult2.map(lambda l:(l,key_face_encoding(l))).map(lambda l:(l[0],np.linalg.norm(A-l[1]))).map(lambda (a,b):(b,a)).sortByKey(ascending=True).map(lambda (a,b):(b,a)).take(imageCount)
			imageCount=0
		else:
			finalDistance_RDD1=finalResult2.map(lambda l:(l,key_face_encoding(l))).map(lambda l:(l[0],np.linalg.norm(A-l[1]))).map(lambda (a,b):(b,a)).sortByKey(ascending=True).map(lambda (a,b):(b,a)).take(resultCount)
			imageCount=imageCount-resultCount
		finalDistanceRDDs=finalDistance_RDD.union(finalDistance_RDD1)
		i=i+1
	#Save as text file
	finalDistanceRDDs.coalesce(1).saveAsTextFile("out_kmeans_compare")
  print("--- %s seconds ---" % (time.time() - start_time))
  sc.stop()
