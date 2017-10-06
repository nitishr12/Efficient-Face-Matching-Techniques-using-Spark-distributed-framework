#pcacompare.py
# Running PCA on image feature set of 128 features 
# Standalone Python/Spark program to perform Principal Component Analysis on the Input image and compare face distance with the M*1 #Image set.
# Performs PCA on D*1 matrix and stores M*1 matrix for Input Image
#
# Loads the Input Image name from the argument 1  
# Loads the M*1 matrices of the Dataset from argument 2 with the help of sequence pickle file
# Loads the mean matrix from the argument 3 for calculation of Xn-mean for Input image
# Loads the principal components matrix from argument 4 for calculation of yn =Xn-meantranspose * principal components 
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
# Usage: spark-submit pcacompare.py <inputImagefilename> <pathTo-yn/part-00000> <pathTo-mean.txt> <pathTo-pc.txt>
# Example usage: spark-submit pcacompare.py gatesjobs1.jpg yn/part-00000 mean.txt pc.txt
#
#

import sys
import face_recognition as fc
import numpy as np
import time
import copy

from pyspark import SparkContext

#Calculate the encoding for each Image name using face_recognition lib and return the encoding
def key_face_encoding(l):
	image = fc.load_image_file(l)
	face_encoding = fc.face_encodings(image)
	face_matrix = np.asmatrix(face_encoding).T
	return face_matrix
#Calculate the facedistance value for image with the input image and return the imagename,facedistance
def face_distance(A,l):
	print "In facedist"
	print l
	print A
	lmatrix = np.asmatrix(l[1])
	Amatrix = np.asmatrix(A[1])
	facedistance=1.0
	shapeA=np.shape(Amatrix)[1]
	shapeL=np.shape(lmatrix) [1]
	print Amatrix
	print lmatrix
	if shapeA==shapeL:
		facedistance=np.linalg.norm(lmatrix-Amatrix) 
	return (l[0],facedistance)
#Calculate Xn-mean for any Image matrix and return imagename,xn-mean
def pca_covariancexnxbar(mean,l) :
	print np.shape(l)
	xnxbar = np.subtract(l,mean.T)
	xnxbart = xnxbar.T
	key= "input"	
	return (key,xnxbar)
#Calculate Yn = xnxbartranspose * principal components for any Image matrix and return the key, YN value
def calculateYN(l,pc):
	xnxbart = np.asmatrix(l[1]).T
	yn=np.dot(xnxbart,pc.T)
	return (l[0],yn)
	


if __name__ == "__main__":
  if len(sys.argv) !=5:
    print >> sys.stderr, "pcacompare.py <inputImagefilename> <pathTo-yn/part-00000> <pathTo-mean.txt> <pathTo-pc.txt>"
    exit(-1)
  stime = time.time()
  sc = SparkContext(appName="PCABasedFaceDistanceComparison")

 
  
  # Four input arguments mentioned above
  strpath = str(sys.argv[1])
  yn = sc.pickleFile(sys.argv[2])
  meanVal = np.asmatrix(np.loadtxt(sys.argv[3]))
  principalcomp = np.asmatrix(np.loadtxt(sys.argv[4]))	
  # Get Face encoding for input matrix
  input_matrix = np.asmatrix(key_face_encoding(strpath))

  yn_RDD = yn.map(lambda l:l)
  ynval = yn_RDD.collect()
  print np.shape(meanVal)
  print np.shape(principalcomp)
  # Get the Xn-mean val for Input Matrix
  inputmeanbar = pca_covariancexnxbar(meanVal,input_matrix)

  print inputmeanbar
  
  print np.shape(np.asmatrix(inputmeanbar[1])) 
  # Get the Yn val for the Input Matrix
  inputyn = calculateYN(inputmeanbar,principalcomp)

  print np.shape(np.asmatrix(inputyn[1]))

  print ynval
  # Calculate the Face distance Value subtracting the M*1 matrices and calculating norm, sort them in ascending order
  face_distance_RDD = yn_RDD.map(lambda l:face_distance(inputyn,l)).map(lambda (a,b):(b,a)).sortByKey(ascending=True).map(lambda (a,b):(b,a))

  face_dist =face_distance_RDD.take(10)
  face_distance_RDD.coalesce(1).saveAsTextFile("out_pca_compare") 
  
  for dist in face_dist:
	print dist 		
  
  print "Time :"
  print time.time()-stime
 	
 	

  sc.stop()
