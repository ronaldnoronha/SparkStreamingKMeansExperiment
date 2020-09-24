# Import Fabric's API module
from fabric.api import sudo
from fabric.operations import reboot
from fabric2 import Connection, Config
from invoke import Responder
from fabric2.transfer import Transfer
import os
from time import sleep
from datetime import datetime

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
        slaveConnections[i].run('source /etc/profile && $SPARK_HOME/sbin/start-slave.sh spark://'+str(masterHost)+':7077')


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


def stop():
    stopSparkCluster()

def runExperiment(clusters='1'):
    # transfer monitor
    transferMonitor()
    # start Monitor
    startMonitor()
    # transfer file
    transfer = Transfer(master)
    # Transfer Producer
    transfer.put('transferFile.py')
    # SBT packaging
    os.system('sbt package')
    # start start cluster
    startSparkCluster(clusters)
    # transfer jar
    transfer.put('./target/scala-2.12/sparkstreamingkmeansexperiment_2.12-0.1.jar')
    print(datetime.now().__str__())
    transferFile(clusters)
    master.run(
            'source /etc/profile && cd $SPARK_HOME && bin/spark-submit '
            '--class example.stream.PredictKMeans '
            '--master spark://' + str(masterHost) + ':7077 --executor-memory 2g '
            '~/sparkstreamingkmeansexperiment_2.12-0.1.jar '
            '450000'
        )
    # transfer logs
    # stopMonitor()
    # transferLogs()
    # Restart all VMs
    stop()
    # restartAllVMs()

def startMonitor():
    for connection in slaveConnections+[master]:
        connection.run('nohup python3 ./monitor.py $1 >/dev/null 2>&1 &')

def stopMonitor():
    for connection in slaveConnections+[master]:
        connection.run('pid=$(cat logs/pid) && kill -SIGTERM $pid')

def transferLogs():
    counter = 1
    for connection in slaveConnections:
        transfer = Transfer(connection)
        transfer.get('logs/log.csv', 'log_slave' + str(counter) + '.csv')
        counter += 1
    transfer = Transfer(master)
    transfer.get('logs/log.csv', 'log_master.csv')


def transferMonitor():
    for connection in slaveConnections+[master]:
        connection.run('rm monitor.py')
        connection.run('rm -rf logs')
        transfer = Transfer(connection)
        transfer.put('monitor.py')
        connection.run('mkdir logs')

def transferToMaster(filename):
    transfer = Transfer(master)
    transfer.put(filename)

def createFile():
    transfer = []
    for connection in slaveConnections:
        transfer.append(Transfer(connection))
    for connection in transfer:
        connection.put('./centers.txt')
        connection.put('./createFile.py')
        connection.put('./transferFile.py')
    for connection in slaveConnections:
        connection.run('tmux new -d -s createFile')
        connection.run('tmux send -t createFile python3\ ~/createFile.py\ 1000000\ 120 ENTER')

def closeCreateFile():
    for connection in slaveConnections:
        connection.run('tmux kill-session -t createFile')

def transferFile(clusters='1'):
    transfer = []
    for i in range(int(clusters)):
        transfer.append(Transfer(slaveConnections[i]))
    for connection in transfer:
        connection.put('./transferFile.py')
    for i in range(int(clusters)):
        slaveConnections[i].run('tmux new -d -s transferFile')
        slaveConnections[i].run('tmux send -t transferFile python3\ ~/transferFile.py ENTER')

def closeTransferFile(clusters='1'):
    for i in range(int(clusters)):
        slaveConnections[i].run('tmux kill-session -t transferFile')
        slaveConnections[i].run('rm ~/data/*.txt')
