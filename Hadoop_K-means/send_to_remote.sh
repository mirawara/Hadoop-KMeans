rm target/Hadoop_K-means-1.0-SNAPSHOT.jar
mvn clean package
scp target/Hadoop_K-means-1.0-SNAPSHOT.jar hadoop@hadoop-namenode:~
