#! /bin/bash

# Start DFS if not already started
if jps | grep -q " NameNode"; then
    echo "Namenode is up and running" 
fi
if jps | grep -q " DataNode"; then
    echo "Datanode is up and running"
else 
    echo "Starting DFS Cluster - two nodes in total"
    hdfs namenode -format
    start-dfs.sh
    echo "Cluster is ready"
fi

# # Start YARN if not already started
if jps | grep -q " NodeManager"; then
    echo "NodeManager up and running" 
fi
if jps | grep -q " SecondaryNameNode"; then
    echo "SecondaryNameNode up and running"
fi
if jps | grep -q " ResourceManager"; then
    echo "RecourceManager up and running"
else 
    echo "Startup YARN modules"
    start-yarn.sh
fi

echo "DFS Cluster is ready!"