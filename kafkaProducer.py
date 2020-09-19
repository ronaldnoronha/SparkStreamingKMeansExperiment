
from json import dumps
from kafka import KafkaProducer
from sklearn.datasets import make_blobs
from datetime import datetime
import csv
import sys
from time import sleep, time
from threading import Thread, Lock

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


def worker(numberOfMessages, centers, producer, results, index):
    t1 = time()
    totalSent = 0
    features, target = create_data(numberOfMessages, 3, centers, 3)
    timeSendKafka = 0

    for i in range(len(features)):
        message = str(datetime.now()) + ',' + ' '.join([str(j) for j in features[i]]) + ',' + str(target[i])
        totalSent += sys.getsizeof(message.encode('utf-8'))
        lock.acquire()
        producer.send('test1', value=message)
        lock.release()

    # t2 = time()



    # t3 = time()
    results[index] = totalSent/(time()-t1)

lock = Lock()
if __name__ == "__main__":
    t1 = time()
    numThreads = 100

    results = [None]*numThreads
    threads = [None]*numThreads
    numberOfMessages = int(sys.argv[1])
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
    producer = KafkaProducer(bootstrap_servers=['192.168.122.121:9092'],
                                     value_serializer = lambda x: dumps(x).encode('utf-8'))


    for i in range(len(threads)):
        threads[i] = Thread(target=worker, args=(numberOfMessages,centers, producer, results, i))
        threads[i].start()

    for i in range(len(threads)):
        threads[i].join()

    print("Average: "+str(sum(results)/len(threads)))
    print("Time: "+str(time()-t1))
