# Copy the Config file
cp /home/waans11/workspace/bigFUN/conf/wisconsin_conf_1node_sqlpp.json /home/waans11/workspace/bigFUN/conf/bigfun-conf_1node_wisconsin.json

# Start
echo `date +%Y%m%d_%H%M%S%3N`": Executing 1node queries... SQLPP" > /home/waans11/workspace/bigFUN/1node_progress.out

# 3303-0011
DIR_NAME_SUFFIX="-Q3303-21-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0011" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-21.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0012
DIR_NAME_SUFFIX="-Q3303-22-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0012" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-22.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0013
DIR_NAME_SUFFIX="-Q3303-23-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0013" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-23.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0014
DIR_NAME_SUFFIX="-Q3303-24-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0024" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-24.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0015
DIR_NAME_SUFFIX="-Q3303-25-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0025" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-25.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0016
DIR_NAME_SUFFIX="-Q3303-26-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0026" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-26.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0017
DIR_NAME_SUFFIX="-Q3303-27-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0027" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-27.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# 3303-0018
DIR_NAME_SUFFIX="-Q3303-28-reddit-comment-1node"
echo `date +%Y%m%d_%H%M%S%3N`": 3303-0028" >> /home/waans11/workspace/bigFUN/1node_progress.out
cp /home/waans11/workspace/bigFUN/files/1node/short100/workload-Q3303-28.txt /home/waans11/workspace/bigFUN/files/1node/workload.txt
/home/waans11/workspace/bigFUN/scripts/run-bigfun-restart-gatherlogs-node1-wisconsin.sh $DIR_NAME_SUFFIX > /home/waans11/workspace/bigFUN/result1node.out 

# done
echo `date +%Y%m%d_%H%M%S%3N`": All queries are executed." >> /home/waans11/workspace/bigFUN/1node_progress.out
