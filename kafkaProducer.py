from time import sleep
from json import dumps
from kafka import KafkaProducer
from sklearn.datasets import make_blobs
from datetime import datetime
from decimal import Decimal
import csv
import sys
import time

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


# import the centers
centers = []
with open('centers.csv','r') as f:
    csvReader = csv.DictReader(f)
    for row in csvReader:
        center = [float(row[i]) for i in row]
        centers.append(center)

producer = KafkaProducer(bootstrap_servers=['192.168.122.54:9092'], value_serializer = lambda x: dumps(x).encode('utf-8'))
totalSent = 0
t1 = time.time()
while True:
    features, target = create_data(8000,3,centers,3)
    for i in range(len(features)):
        message = str(datetime.now()) + ',' + ' '.join([str(j) for j in features[i]]) + ',' + str(target[i])
        totalSent += sys.getsizeof(message)
        producer.send('test', value=message)
        # print(message)

    t2= time.time()
    print(t2)
    print('Total sent:')
    print(str(totalSent)+' bytes in '+str(t2-t1)+ ' seconds')
