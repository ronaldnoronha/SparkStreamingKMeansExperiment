from time import sleep
from json import dumps
from kafka import KafkaProducer
from sklearn.datasets import make_blobs
from datetime import datetime
from decimal import Decimal
import csv

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

features, target = create_data(800,3,centers,3)
producer = KafkaProducer(bootstrap_servers=['192.168.122.54:9092'], value_serializer = lambda x: dumps(x).encode('utf-8'))

with open('testData.csv','w') as f:
    fieldnames = ['x', 'y', 'z', 'target']
    writer = csv.DictWriter(f, fieldnames)
    writer.writeheader()
    for i in range(len(features)):
        dct = {}
        for j in range(len(features[i])):
            dct[fieldnames[j]] = features[i][j]
        dct[fieldnames[len(features[i])]] = target[i]
        writer.writerow(dct)
        message = str(datetime.now()) + ',' + ' '.join([str(j) for j in features[i]]) + ',' + str(target[i])
        print(message)
        producer.send('test', value=message)

features, target = create_data(800,3,centers,3)

with open('testData.csv','a') as f:
    fieldnames = ['x', 'y', 'z', 'target']
    writer = csv.DictWriter(f, fieldnames)
    writer.writeheader()
    for i in range(len(features)):
        dct = {}
        for j in range(len(features[i])):
            dct[fieldnames[j]] = features[i][j]
        dct[fieldnames[len(features[i])]] = target[i]
        writer.writerow(dct)
        message = str(datetime.now()) + ',' + ' '.join([str(j) for j in features[i]]) + ',' + str(target[i])
        print(message)
        producer.send('test', value=message)










