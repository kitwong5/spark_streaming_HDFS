Setup:
1) use WinSCP to transfer follow files from your PC to jumphost /home/ec2-user/data/ 
	count.jar
2) use Putty to login the jumphost sever
3) create cluster
4) copy file to cluster from jumphost
5) login to cluster

Run Steps:
1) run follow script to create input and output folders
	hadoop fs -rm -f -r /input
	hadoop fs -rm -f -r /output
	hadoop fs -mkdir /input
	hadoop fs -mkdir /output
	
2) run follow script to execute
	spark-submit --class streaming.WordCount --master yarn --deploy-mode client count.jar hdfs:///input hdfs:///output

3) run follow script copy testing text files for counting to the input folder
	hadoop fs -copyFromLocal test1.txt /input/test1.txt
	...
	
4) run follow script to check the execution output
	hadoop fs -ls /output /
  hadoop fs -cat /output/taskA-001/part*

