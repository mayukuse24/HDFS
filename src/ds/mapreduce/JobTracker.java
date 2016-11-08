package ds.mapreduce;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import ds.mapreduce.maprformat.JobSubmitRequest;
import ds.mapreduce.maprformat.JobStatusRequest;
import ds.mapreduce.maprformat.JobStatusResponse;
import ds.mapreduce.maprformat.JobSubmitResponse;
import ds.hdfs.Client;
import ds.hdfs.hdfsformat.BlockLocationRequest;
import ds.hdfs.hdfsformat.BlockLocationResponse;
import ds.hdfs.hdfsformat.OpenFileRequest;
import ds.hdfs.hdfsformat.OpenFileResponse;
import ds.hdfs.*;

public class JobTracker implements IJobTracker{

	
	Client hdfsclient;
	
	public class Job
	{
		int jid;
		int numofreducetasks,numofmaptasks;
		int completedreducetasks,completedmaptasks;
		int inputfilehandle; //TODO:required to close hdfs file in end
		String mapName,reducerName,inputFile,outputFile;
		
		public Job(int id,int nummt,int numrt,String mname,String rname,String infile,String outfile,int fhandle)
		{
			jid = id;
			numofmaptasks = nummt;
			numofreducetasks = numrt;
			mapName = mname;
			reducerName = rname;
			inputFile = infile;
			outputFile = outfile;
			inputfilehandle = fhandle;
			
			completedreducetasks = completedmaptasks = 0;
		}
	}
	
	public class MapTask
	{
		int tid,jid;
		String mapName,inputFile;
		int inputchunkno;
		boolean taken;
		public MapTask(int id1,int id2,int cno,String mname)
		{
			tid = id1;
			jid = id2;
			inputchunkno = cno;
			mapName = mname;
		}
	}
	
	public class ReduceTask
	{
		int tid,jid;
		int numofreducetasks;
		String mapName,reducerName;
		boolean taken;
		public ReduceTask(int id1,int id2,int num,String mname,String rname)
		{
			tid = id1;
			jid = id2;
			numofreducetasks = num;
			mapName = mname;
			reducerName = rname;
		}
	}
	
	public class DataNode
	{
		String ip;
		int port;
		String serverName;
		public DataNode(String addr,int p,String sname)
		{
			ip = addr;
			port = p;
			serverName = sname;
		}
	}
	
	Queue<Job> jobqueue = new LinkedList<Job>();
	Queue<MapTask> maptaskqueue = new LinkedList<MapTask>();
	Queue<ReduceTask> reducetaskqueue = new LinkedList<ReduceTask>();
    
    private boolean findJobInQueue(Queue<Job> qt,int num)
    {
    	for(Job item : qt)
    	{
    		if(item.jid == num)
    		{
    			return true;
    		}
    	}
    	return false;
    }
    
    private boolean findMapTaskInQueue(Queue<MapTask> qt,int num)
    {
    	for(MapTask item : qt)
    	{
    		if(item.jid == num)
    		{
    			return true;
    		}
    	}
    	return false;
    }
    
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
 	public byte[] jobSubmit(byte[] inpdata) throws RemoteException
	{
		JobSubmitResponse.Builder response = JobSubmitResponse.newBuilder();
		try
		{
			JobSubmitRequest request = JobSubmitRequest.parseFrom(inpdata);
			
			int random = (int)((Math.random() * 10000) + 1); //get random job id
			while(findJobInQueue(jobqueue,random))
			{
				random++;
			}
			
			OpenFileRequest openfilerequest = OpenFileRequest.newBuilder().setFileName(request.getInputFile())
					.setForRead(true).build();
			
			//find out number of maptasks
			OpenFileResponse openfileresponse = OpenFileResponse.parseFrom(this.hdfsclient.NNStub
					.openFile(openfilerequest.toByteArray()));
			
			BlockLocationRequest.Builder blocrequest = BlockLocationRequest.newBuilder();
			//find DNs of chunks
			for(int i=0;i<openfileresponse.getBlockNumsCount();i++)
			{
				blocrequest.addBlockNums(openfileresponse.getBlockNums(i));
			}
			BlockLocationResponse blocresponse = BlockLocationResponse.parseFrom(this.hdfsclient.NNStub
					.getBlockLocations(blocrequest.build().toByteArray()));
			
			// Create and add new job
			Job newjob = new Job(random,openfileresponse.getBlockNumsCount(),request.getNumReduceTasks(),request.getMapName(),request.getReducerName(),
					request.getInputFile(),request.getOutputFile(),openfileresponse.getHandle());
									
			jobqueue.add(newjob);
						
			//create and add map tasks
			for(int j=0;j<blocresponse.getBlockLocationsCount();j++)
			{
				int random2 = (int)((Math.random() * 10000) + 1); //get random task id
				while(findMapTaskInQueue(maptaskqueue,random2))
				{
					random2++;
				}
				MapTask newmaptask = new MapTask(random2,random,blocresponse.getBlockLocations(j).getBlockNumber(),request.getMapName());
				maptaskqueue.add(newmaptask);
			}
			//TODO: decide whether map tasks DN locations to be stored or retrieved in HBresponse on demand
			response.setJobId(random);
			response.setStatus(1);
			
		}catch(Exception e)
        {
            System.out.println("Error " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }		
	
		return response.build().toByteArray();
	}

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	public byte[] getJobStatus(byte[] inpdata) throws RemoteException
	{
		JobStatusResponse.Builder response = JobStatusResponse.newBuilder();
		try
		{
			JobStatusRequest request = JobStatusRequest.parseFrom(inpdata);
			
			//job not done by default
			response.setJobDone(false);
			response.setTotalMapTasks(0).setTotalReduceTasks(0);
			for(Job item : jobqueue)
			{
				if(item.jid == request.getJobId())
				{
					response.setTotalMapTasks(item.numofmaptasks).setTotalReduceTasks(item.numofreducetasks);
					
					int mapcount=0;
					for(MapTask mapitem : maptaskqueue)
					{
						if(mapitem.jid == item.jid && mapitem.taken)
							mapcount++;
					}
					
					int reducecount = 0;
					for(ReduceTask reduceitem : reducetaskqueue)
					{
						if(reduceitem.jid == item.jid && reduceitem.taken)
							reducecount++;
					}
					
					response.setNumMapTasksStarted(mapcount).setNumReduceTasksStarted(reducecount);
					
					if(item.completedmaptasks == item.numofmaptasks && item.completedreducetasks == item.numofreducetasks)
						response.setJobDone(true);
				}
			}
			response.setStatus(1);
			
		}
		catch(Exception e)
		{
			System.out.println("Error " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	public byte[] heartBeat(byte[] inpdata) throws RemoteException
	{
		
		
		return null;
	}
	
	public static void main(String[] args)
	{
		/*
		// To read config file and Connect to NameNode
        //Intitalize the Client
		JobTracker Jtobj = new JobTracker();
        Jtobj.hdfsclient = new Client();
        System.out.println("Acquiring NameNode stub");

        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
        String Config = Jtobj.hdfsclient.FileTail("nn_details.txt");
        String[] Split_Config = Config.split(";");
        Jtobj.hdfsclient.NNStub = Jtobj.hdfsclient.GetNNStub(Split_Config[0], Split_Config[1], Integer.parseInt(Split_Config[2]));
        */
		
		try
		{
			String pathToJar = "/home/mayukuse/java/HDFS/src/jarnewtest.jar";
			JarFile jarFile = new JarFile(pathToJar);	

			Enumeration<JarEntry> e = jarFile.entries();
			while(e.hasMoreElements())
			{
				System.out.println(e.nextElement().getName());	
			}
			
			URL[] urls = { new URL("jar:file:" + pathToJar+"!/") };
			URLClassLoader cl = URLClassLoader.newInstance(urls);

		    Class<?> c = cl.loadClass("ds.mapreduce.Mapper");
		    
		    System.out.println(c.getMethod("map",String.class).invoke(c.newInstance(),"ola"));
		    
		}catch(Exception e)
        {
            System.out.println("Error " + e.toString());
            e.printStackTrace();
        }
	}
}
