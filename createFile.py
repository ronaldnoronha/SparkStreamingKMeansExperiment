from sklearn.datasets import make_blobs
from datetime import datetime
import sys
from time import sleep, time
import os


def create_data(n_samples, n_features, centers, std):
    features, target = make_blobs(n_samples = n_samples,
                                  # two feature variables,
                                  n_features = n_features,
                                  # four clusters,
                                  centers = centers,
                                  # with .65 cluster standard deviation,
                                  cluster_std = std,
                                  # shuffled,
                                  shuffle = True)
    return features, target



if __name__ == "__main__":

    numberOfMessages = int(sys.argv[1])
    numberOfFiles = int(sys.argv[2])
    # read the centers file
    centers = []
    with open('centers.txt','r') as f:
        line = f.readline()
        while line:
            centers.append([float(i) for i in line.split(' ')])
            line = f.readline()

    t1 = time()
    destPath = '/home/ronald/dataTemp/'
    if not os.path.exists(destPath):
        os.makedirs(destPath)
    for i in range(numberOfFiles):
        features, target = create_data(numberOfMessages, 3, centers, 0.65)
        with open(destPath+str(i)+'.txt','w') as f:
            for i in range(len(features)):
                f.write(' '.join([str(j) for j in features[i]]) + ',' + str(target[i])+"\n")

    print('{} points saved'.format(numberOfMessages))
    print('{} seconds'.format(time()-t1))

