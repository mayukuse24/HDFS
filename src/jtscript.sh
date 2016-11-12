javac ds/mapreduce/JobTracker.java ds/mapreduce/IJobTracker.java 

while /bin/true; do
    java ds.mapreduce.JobTracker
    sleep 2s
done
