# kmeans.py
# 
# Standalone Python/Spark program to perform clustering using kmeans.
# Performs clustering on finding the norm between two faces and the recomputes 
# the cluster centers on taking an average on all the matrix of the initial cluster and the subsequent clusters.
# This process continues till the objects in clusters are not likely to change
# ITCS 6190 - Cloud Computing - Final Project
# Nitish Rangarajan
# 800963268
# nrangara@uncc.edu
# Usage: spark-submit kmeans.py <inputdatafile> <# of clusters>
# Example usage:  spark-submit kmeans.py minten200/ 10
#
#

import sys
import face_recognition as fc
import numpy as np
import copy
import random

from pyspark import SparkContext


def key_face_encoding(l):
	# This returns the face encoding of the given image in l
	print "Inside face_encoding"
	#print l
	image = fc.load_image_file(l)
	face_encoding = fc.face_encodings(image)
	face_matrix = np.asmatrix(face_encoding)
	#print face_matrix
	return face_matrix

def face_distance(A1,l):
	# This returns the cluster with minimum face distance for each cluster center in A and the image in l
	print "Inside face distance"
	#print l[1]
	facedistance=[1.0 for x in range(len(A1))]
	for index in range(len(A1)):
		A=A1[index]
		lmatrix = np.asmatrix(l[1])
		shapeA=np.shape(A)[1]
		shapeL=np.shape(lmatrix) [1]
		#print shapeA
		#print shapeL
		#This is to check if both images have the same dimension
		if shapeA==shapeL:
			facedistance[index]=np.linalg.norm(A-lmatrix) 
	
	for index in range(len(facedistance)):
		if facedistance[index]==min(facedistance):	
			minimum=index
	return minimum+1


def clusters(l,noOfClusters):
#This function sets up the initial cluster centers on taking k random images from the images
	#lmatrix=np.matrix(l)
	print "Computing Clusters"
	print l
	l=np.array(l)
	w = noOfClusters;
	clusters=[0 for x in range(w)]
	for index in range(noOfClusters):
		l1=random.choice(l)
		clusters[index]=l1
	#for dist in clusters:
  	#	print dist
	return clusters


def computeResults(l,A):
#This function returns the key value pair of cluster centers with the image file names
	name=l[0]
	vector=np.asmatrix(l[1])
	for list in A:
		vector1=list[1]
		if np.array_equal(vector,vector1):
			cluster=list[0]
	return (cluster,name)

if __name__ == "__main__":
  if len(sys.argv) !=3:
    print >> sys.stderr, "Usage: spark-submit kmeans.py <inputdatafile> <# of clusters>"
    exit(-1)

  sc = SparkContext(appName="KMeansClusterCreation")

  imageset = images = sc.wholeTextFiles(sys.argv[1])
  strpath = str(sys.argv[1])	
  noOfClusters=int(sys.argv[2])
  converge=False
  iteration=0

  #Find all the images in the path, find the encoding for each
  B_RDD= imageset.map(lambda l:l).map(lambda l1: strpath+(str(l1).split(',')[0].split(strpath)[1] [:-1])).map(lambda l2: (l2,key_face_encoding(l2))).filter(lambda l :np.shape(l[1])[1]==128)
  B=B_RDD.collect()
  
  C=B_RDD.map(lambda l:l[1]).collect()

  #Find the initial cluster centers
  clusterCenters=clusters(C,noOfClusters)
  for dist in clusterCenters:
  	print dist
  #Gives the cluster # after 1st iteration

  while(converge==False):
	  #Find the best cluster for each image
	  D_RDD=B_RDD.map(lambda l:(face_distance(clusterCenters,l),l[1]))
	  D_sample=D_RDD.map(lambda l:l[0]).collect()
	  print D_sample
	  Dmatrix=np.matrix(D_sample)

	  #Sum all the encodings of a cluster
	  D_initial=D_RDD.combineByKey(lambda value: (value, 1),
		                    lambda x, value: (x[0] + value, x[1] + 1),
		                    lambda x, y: (x[0] + y[0], x[1] + y[1])
		                   )
	  #Find the average of the sum and assign it to be the new cluster centers
	  D=D_initial.map(lambda (key, (totalSum, count)): (key, totalSum / count))
	  D_clusterCenters=D.map(lambda l:l[1]).collect()
	  clusterCenters=np.array(D_clusterCenters)
	  
	  #Find the best cluster for each image	
	  D_RDD1=B_RDD.map(lambda l:(face_distance(clusterCenters,l),l[1]))
	  D_sample1=D_RDD.map(lambda l:l[0]).collect()
	  print D_sample1
	  Dmatrix1=np.matrix(D_sample1)
	  #Check if the images have the same cluster centers. If yes, then it has converged.Reiterate if not
	  if np.array_equal(Dmatrix,Dmatrix1):
		converge=True
		print "Has Converged"
	  iteration=iteration+2
	  print "In iteration "+ str(iteration)
	  
  Dcheck=D_RDD1.collect()
  #Get the cluster centers and their images
  Results=B_RDD.map(lambda l:computeResults(l,Dcheck)).groupByKey().map(lambda x : (x[0], list(x[1])))
  #Save the results
  Results.saveAsPickleFile("Results")
  cluster_RDD=sc.parallelize(clusterCenters)
  #Save cluster centers
  cluster_RDD.saveAsPickleFile("Cluster")
  
  sc.stop()
