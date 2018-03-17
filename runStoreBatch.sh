#localPath = sys.argv[1]
#hdfsPath = sys.argv[2]
#inputPath = sys.argv[3]
#logfile = sys.argv[4]
python runStore.py dbout-10 spartidb-10 extvpdb-10/dataset-watdiv-sf10-prefix.bz2 watdiv output-10.log
python runStore.py dbout-100 spartidb-100 extvpdb-100/dataset-watdiv-sf100-prefix.bz2 watdiv output-100.log
python runStore.py dbout-1000 spartidb-1000 extvpdb-1000/dataset-watdiv-sf1000-prefix.bz2 watdiv output-1000.log