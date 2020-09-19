# Import Fabric's API module
from fabric.api import sudo
from fabric.operations import reboot
from fabric2 import Connection, Config
from invoke import Responder
from fabric2.transfer import Transfer
import os
from time import sleep

with open('./conf/master', 'r') as f:
    array = f.readline().split()
    masterHost = array[0]
    masterPort = array[1]
    user = array[2]
    host = array[3]

config = Config(overrides={'user': user})
conn = Connection(host=host, config=config)
configMaster = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
master = Connection(host=masterHost, config=configMaster, gateway=conn)

slaveConnections = []
configSlaves = Config(overrides={'user': user, 'connect_kwargs': {'password': '1'}, 'sudo': {'password': '1'}})
with open('./conf/slaves', 'r') as f:
    array = f.readline().split()
    while array:
        slaveConnections.append(Connection(host=array[0], config=configSlaves, gateway=conn))
        array = f.readline().split()
with open('./conf/kafka', 'r') as f:
    array = f.readline().split()
    kafka = Connection(host=array[0], config=Config(overrides={'user': user,
                                                                         'connect_kwargs': {'password': '1'},
                                                                         'sudo': {'password': '1'}}), gateway=conn)
sudopass = Responder(pattern=r'\[sudo\] password:',
                     response='1\n',
                     )

def startSparkCluster(n='1'):
    # start master
    master.run('source /etc/profile && $SPARK_HOME/sbin/start-master.sh')
    # start slaves
    for i in range(int(n)):
        slaveConnections[i].run('source /etc/profile && $SPARK_HOME/sbin/start-slave.sh spark://192.168.122.54:7077')


def stopSparkCluster():
    master.run('source /etc/profile && $SPARK_HOME/sbin/stop-all.sh')
    # c2.run('cd /usr/local/spark && ./sbin/stop-all.sh')


def restartAllVMs():
    for connection in slaveConnections:
        try:
            connection.sudo('shutdown -r now')
        except:
            continue
    try:
        master.sudo('shutdown -r now')
    except:
        pass
    try:
        kafka.sudo('shutdown -r now')
    except:
        pass



def startKafka(dataSize='1000'):
    kafka.run('tmux new -d -s kafka')
    kafka.run('tmux new-window')
    kafka.run('tmux new-window')
    kafka.run('tmux send -t kafka:0 /home/ronald/kafka_2.12-2.5.0/bin/zookeeper-server-start.sh\ '
               '/home/ronald/kafka_2.12-2.5.0/config/zookeeper.properties ENTER')
    sleep(5)
    kafka.run('tmux send -t kafka:1 /home/ronald/kafka_2.12-2.5.0/bin/kafka-server-start.sh\ '
               '/home/ronald/kafka_2.12-2.5.0/config/server.properties ENTER')
    kafka.run('tmux send -t kafka:2 python3\ /home/ronald/kafkaProducer.py\ ' + dataSize + ' ENTER')



def stopKafka():
    kafka.run('tmux kill-session -t kafka')


def stop():
    stopKafka()
    stopSparkCluster()

def runExperiment(dataSize='1000',clusters='1'):
    # transfer monitor
    transferMonitor()
    # start Monitor
    startMonitor()
    # transfer file
    transfer = Transfer(master)
    transferKafka = Transfer(kafka)
    # Transfer producer
    transferKafka.put('kafkaProducer.py')
    # start kafka
    startKafka(dataSize)
    # SBT packaging
    os.system('sbt package')
    # start start cluster
    startSparkCluster(clusters)
    # Transfer files to master
    transferKafka.get('/home/ronald/centers.csv')
    transfer.put('./centers.csv')
    # transfer jar
    transfer.put('./target/scala-2.12/sparkstreamingkmeansexperiment_2.12-0.1.jar')

    master.run(
            'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
            '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0 '
            '--class example.stream.PredictKMeans '
            '--master spark://' + str(masterHost) + ':7077 --executor-memory 2g '
            '~/sparkstreamingkmeansexperiment_2.12-0.1.jar '
            '192.168.122.121:9092 '
            'consumer-group '
            'test1 '
            '1000000'
        )

    # transfer logs
    stopMonitor()
    transferLogs()
    # Restart all VMs
    stop()
    # restartAllVMs()

def startMonitor():
    for connection in slaveConnections+[master, kafka]:
        connection.run('nohup python3 ./monitor.py $1 >/dev/null 2>&1 &')

def stopMonitor():
    for connection in slaveConnections+[master, kafka]:
        connection.run('pid=$(cat logs/pid) && kill -SIGTERM $pid')

def transferLogs():
    counter = 1
    for connection in slaveConnections:
        transfer = Transfer(connection)
        transfer.get('logs/log.csv', 'log_slave' + str(counter) + '.csv')
        counter += 1
    transfer = Transfer(master)
    transfer.get('logs/log.csv', 'log_master.csv')
    transfer = Transfer(kafka)
    transfer.get('logs/log.csv', 'log_kafka.csv')



def transferMonitor():
    for connection in slaveConnections+[master, kafka]:
        connection.run('rm monitor.py')
        connection.run('rm -rf logs')
        transfer = Transfer(connection)
        transfer.put('monitor.py')
        connection.run('mkdir logs')

def transferToKafka(filename):
    transfer = Transfer(kafka)
    transfer.put(filename)
