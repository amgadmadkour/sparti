import sys, getopt, subprocess, time, os, errno


def submitSparkCommand(localPath, hdfsPath, inputPath, dataset):
    if not hdfsPath.endswith("/"):
        hdfsPath += "/"

    if not localPath.endswith("/"):
        localPath += "/"

    command = ("spark-submit "
               + "--driver-memory 16g "
               + "--master spark://172.18.11.128:7077 "
               + "--executor-memory 20g "
               + "--class edu.purdue.sparti.queryprocessing.storage.SpartiStoreCreator "
               + "target/uber-sparti-1.0-SNAPSHOT.jar "
               + "-l " + localPath + " "
               + "-r hdfs://172.18.11.128:8020/user/amadkour/" + hdfsPath + " "
               + "-d hdfs://172.18.11.128:8020/user/amadkour/" + inputPath + " "
               + "-t spark "
               + "-b " + dataset + " "
               + "-s space")

    start = int(round(time.time()))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    end = int(round(time.time()))
    total = "{:.0f}".format(end - start)
    return total


if __name__ == "__main__":
    localPath = sys.argv[1]
    hdfsPath = sys.argv[2]
    inputPath = sys.argv[3]
    dataset = sys.argv[4]
    logfile = sys.argv[5]

    LOG = open(logfile, "w")

    total = submitSparkCommand(localPath=localPath, hdfsPath=hdfsPath, inputPath=inputPath, dataset=dataset)
    LOG.write("Dataset : %s \n" % (inputPath))
    LOG.write("Time Taken: %s sec \n" % (total))
    LOG.close()
