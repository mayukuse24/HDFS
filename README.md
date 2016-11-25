This is the work of Shaleen Garg and Vinaya Khandelwal

# HDFS + MapReduce

# System configuration

The project was tested using **docker** instances. See docker at https://www.docker.com/ .
Some basic commands.

docker images
docker run --rm -ti "image(ds/arch)" /bin/bash 

(proxy version of run command. net=host tells docker instance to use the host network settings(ip,port) )
docker run --name yourdock -e http_proxy -e https_proxy --net=host --rm -ti "image(ds/arch)" /bin/bash

docker exec -i -t yourdock /bin/bash (Run two or more tabs of same docker instance)

docker inspect -f '{{.Name}} - {{.NetworkSettings.IPAddress }}' $(docker ps -aq) (test )

# Instructions

The project contains three folders bin, src and util.

**bin** contains the compiled code

**src** contains the java files structured as follows :

ds/hdfs for the HDFS java files
ds/mapreduce for the Mapreduce java files

### Configuration files

jt_details.txt -- stores information of Jobtracker ip and port
nn_details.txt -- stores information of Namenode ip and port 
TT_details.txt -- stores information of Tasktracker ID, NumberOfMapThreads and NumberOfReduceThreads
dn_config.txt -- stores information of Datanode ID, ip and port
jar file contains the map functions and the reduce functions. They are dynamically loaded.
REGEX file contains the grep term.

Every Node needs the nn_details.txt file to contact Namenode. The rest of the files are required by 
respective nodes for binding to their own ips and ports.

**util** folder contains : 

jtscript.sh -- Script to run Jobtracker. 
nnscript.sh -- Script to run NameNode.

NOTE : Both the scripts need to be moved and executed from the src folder.




