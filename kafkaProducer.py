
from json import dumps
from kafka import KafkaProducer
from sklearn.datasets import make_blobs
from datetime import datetime
import csv
import sys
from time import sleep
from threading import Thread

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

class ProducerWorker(Thread):

    def __init__(self,number, dataSize, centers):
        Thread.__init__(self)
        self.title = number
        self.producer = KafkaProducer(bootstrap_servers=['192.168.122.121:9092'],
                                     value_serializer = lambda x: dumps(x).encode('utf-8'))
        self.dataSize = dataSize
        self.centers = centers

    def run(self):
        totalSent = 0
        print('Thread ' + str(self.dataSize) + ' sent ' + str(totalSent) + ' bytes')
        while totalSent < self.dataSize:
            features, target = create_data(8, 3, centers, 3)
            for i in range(len(features)):
                message = str(datetime.now()) + ',' + ' '.join([str(j) for j in features[i]]) + ',' + str(target[i])
                totalSent += sys.getsizeof(message)
                self.producer.send('test', value=message)

        print('Thread '+str(self.title)+' sent '+str(totalSent)+' bytes')

if __name__ == "__main__":
    amountOfData = int(sys.argv[1])

    print('Producing :'+ str(amountOfData)+' bytes of data')
    amountOfDataPerThread = amountOfData/4

    # Create Centers for production
    centers, cluster_num = create_data(8, 3, 8, 3)
    with open('centers.csv', 'w') as f:
        fieldnames = ['x', 'y', 'z']
        writer = csv.DictWriter(f, fieldnames)
        writer.writeheader()
        for i in range(len(centers)):
            dct = {}
            for j in range(len(centers[i])):
                dct[fieldnames[j]] = centers[i][j]
            #         dct[fieldnames[len(centers[i])]] = cluster_num[i]
            writer.writerow(dct)
    sleep(5)
    # Start thread
    for i in range(4):
        worker = ProducerWorker(i, amountOfDataPerThread, centers)
        worker.start()
