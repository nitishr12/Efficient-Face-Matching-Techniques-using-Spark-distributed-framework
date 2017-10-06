********************************************************README file******************************************************************

1. Move the images to the hadoop directory
  hadoop fs -put ~/Downloads/minten200 /user/nitish/

	STEP 1 - Linear Compare
2. To run the linearcompare.py, you need the path to the input image and path to the image dataset
  spark-submit linearcompare.py obama.jpg minten200/

	STEP 2 - K-Means Algorithm
3. To run the kmeans.py, you need the path to the image dataset and the number of clusters. The number of clusters depends on the number of personalities in the image dataset. This may change
  spark-submit kmeans.py minten200/ 10

	STEP 3 - Search image with the clusters formed from k-means algorithm
4. To run the kmeanswithinput.py, you need the path to the input image. The input dataset will be fetched from picklefile from the earlier step
  spark-submit kmeans.py minten200/gates_1.jpg

	STEP 4 - PCA Dimensionality Reduction
5. To run the pcadataset.py, you need the path to the image dataset and the M value for reducing the [128*1] matrix. This will be reduced to[M*1]
  spark-submit pcadataset.py minten200/ 7 

	STEP 5 - Search image with the new dimensions formed from PCA
6. To run the pcacompare.py, you need the path to the input image, the path to the picklefile that has reduced dimensions of the image dataset formed from the previous step, path to the picklefile that has the average image vectors formed from the previous step and path to the picklefile that has the principal components formed from the previous step.
  spark-submit pcacompare.py gatesjobs1.jpg yn/part-00000 mean.txt pc.txt

Note: Step 1, Step 3 and Step 5 will save the image name, facedistance in a key valued pairs to an output folder
