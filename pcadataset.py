#pcadataset.py
# Running PCA on image feature set of 128 features on the dataset 
# Standalone Python/Spark program to perform Principal Component Analysis on the Dataset images and store the mean, pricipal #components and yn values for the dataset
# Performs PCA on D*1 matrix and stores M*1 matrix for Input Image
#
# Loads the imageset folder from the argument 1  
# Load the M value from input argument
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
# Usage: spark-submit pcadataset.py <pathTo-imageset> <M-val(Integer)>
# Example usage: spark-submit pcadataset.py minten200/ 7 
#
#
import sys
import face_recognition as fc
import numpy as np
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
	lmatrix = np.asmatrix(l[1])
	Amatrix = np.asmatrix(A)
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
	key =l[0]
	print key
	xnxbar = np.subtract(l[1],mean)
	xnxbart = xnxbar.T	
	return (key,xnxbar)
#Calculate the covariance matrix for each image i.e xn-xbar * xn-xbartranspose /N
def covarince(n,l) :
	key =l[0]
	print key
	transposeMatr = np.asmatrix(l[1]).T
	symmMatr = np.dot(l[1],transposeMatr)
	variance = np.true_divide(symmMatr,n)
	return (key,variance)
#Calculate Yn = xnxbartranspose * principal components for any Image matrix and return the key, YN value
def calculateYN(l,pc):
	xnxbart = np.asmatrix(l[1]).T
	yn=np.dot(xnxbart,pc.T)
	return (l[0],yn)
	


if __name__ == "__main__":
  if len(sys.argv) !=3:
    print >> sys.stderr, "pcadataset.py <pathTo-imageset> <M-val(Integer)>"
    exit(-1)

  sc = SparkContext(appName="PCAOnDataset")

 
  imageset = sc.wholeTextFiles(sys.argv[1])
 
  strpath = str(sys.argv[1])
  M = int(sys.argv[2])	

  


  
  # Collect matrices for all images in the dataset
  B_RDD = imageset.map(lambda l:l).map(lambda l1: strpath+(str(l1).split(',')[0].split(strpath)[1] [:-1])).map(lambda l2: (l2,key_face_encoding(l2))).filter(lambda l: np.shape(l[1])[0]==128);

  B = B_RDD.collect();
  # calculate the sum of the matrices
  meanSum = B_RDD.map(lambda l:l[1]).sum()  

  

 
 
  
  #print "B file"
  #print Bfile		
  N = len(B)

  #Calculate the mean value
  meanVal = np.true_divide(meanSum,N)
  print "Mean Val"
  print meanVal  
  # Calculate xn-Xbar for each image
  XnXbar_RDD = B_RDD.map(lambda l:pca_covariancexnxbar(meanVal,l))
  # Calculate Covariance matrix = 1/n summation of xn-xbar *xn-xbartranspose
  covarianceVal = XnXbar_RDD.map(lambda l : covarince(N,l)).map(lambda l1:l1[1]).sum()
  print "Covariance"
  print np.shape(covarianceVal)
  col = np.shape(covarianceVal)[1]
  # Calculate the eigen value and eigen vectors
  eigVal , eigVector = np.linalg.eigh(covarianceVal)

  print np.shape(eigVal)
  print np.shape(eigVector)
  print eigVal
  # Get the index values for eigval
  Eigindx = np.argsort(eigVal)
  
  print Eigindx
  print Eigindx[-1:-(col+1):-1]
  
  print eigVal[Eigindx[-1:-(col+1):-1]]
  # Reverse the eigVector so that it is in descending order
  eigVector = eigVector.T[-1:-(col+1):-1]
  # take the M principal components alone from the eigVector
  principalcomponents = eigVector[0:M] 
  print np.trace(covarianceVal)
  print sum(eigVal)
  print np.shape(principalcomponents)
  # Save the mean and principal components in a text file
  np.savetxt('mean.txt',meanVal)
  np.savetxt('pc.txt',principalcomponents)
  # Calculate the Yn M*1 matrix for each image in the dataset
  yn_RDD = XnXbar_RDD.map(lambda l:calculateYN(l,principalcomponents))
  # Save the Yn values as a Pickle File
  yn_RDD.saveAsPickleFile("yn")
  
  yn = yn_RDD.collect()
	
  for f in yn:
	print f	
	print np.shape(f[1])
  print len(yn)

  
  
 	

  sc.stop()
