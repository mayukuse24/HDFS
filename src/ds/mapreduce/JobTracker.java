package ds.mapreduce;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ds.mapreduce.maprformat.JobSubmitRequest;
import ds.mapreduce.maprformat.DataNodeLocation;
import ds.mapreduce.maprformat.HeartBeatRequest;
import ds.mapreduce.maprformat.JobStatusRequest;
import ds.mapreduce.maprformat.JobStatusResponse;
import ds.mapreduce.maprformat.JobSubmitResponse;
import ds.mapreduce.maprformat.MapTaskInfo;
import ds.mapreduce.maprformat.ReducerTaskInfo;
import ds.hdfs.Client;
import ds.hdfs.hdfsformat.BlockLocationRequest;
import ds.hdfs.hdfsformat.BlockLocationResponse;
import ds.hdfs.hdfsformat.BlockLocations;
import ds.hdfs.hdfsformat.HeartBeatResponse;
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
		boolean reduceStage;
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
			reduceStage = false;
			completedreducetasks = completedmaptasks = 0;
		}
	}
	
	public class TaskData
	{
		int tid,jid;
	}
	
	public class MapTask extends TaskData
	{
		String mapName,inputFile;
		int inputchunkno;
		boolean taken;
		ArrayList<DataNode> Dnlist;
		public MapTask(int id1,int id2,int cno,String mname)
		{
			tid = id1;
			jid = id2;
			inputchunkno = cno;
			mapName = mname;
			Dnlist = new ArrayList<DataNode>();
			taken=false;
		}
	}
	
	public class ReduceTask extends TaskData
	{
		String reducerName,outputfile;
		boolean taken; //check if task started
		ArrayList<String> filenamelist;
		public ReduceTask(int id1,int id2,String rname,String outfile)
		{
			tid = id1;
			jid = id2;
			reducerName = rname;
			taken=false;
			filenamelist = new ArrayList<String>();
			outputfile = outfile;
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
	
	ArrayList<Job> jobqueue = new ArrayList<Job>();
	ArrayList<MapTask> maptaskqueue = new ArrayList<MapTask>();
	ArrayList<ReduceTask> reducetaskqueue = new ArrayList<ReduceTask>();
    ArrayList<Integer> tasktrackerlist = new ArrayList<Integer>();
    
    private boolean findJobInQueue(ArrayList<Job> qt,int num)
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
    
    private boolean findTaskInQueue(ArrayList<? extends TaskData> qt,int num)
    {
    	for(TaskData item : qt)
    	{
    		if(item.tid == num)
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
				while(findTaskInQueue(maptaskqueue,random2))
				{
					random2++;
				}
				MapTask newmaptask = new MapTask(random2,random,blocresponse.getBlockLocations(j).getBlockNumber(),request.getMapName());
				
				//Store Datanode information of chunks in maptask itself
				for(int k=0; k<blocresponse.getBlockLocations(j).getLocationsCount();k++)
				{
					DataNode tobj = new DataNode(blocresponse.getBlockLocations(j).getLocations(k).getIp(),
							blocresponse.getBlockLocations(j).getLocations(k).getPort(),blocresponse.getBlockLocations(j).getLocations(k).getName());
					
					newmaptask.Dnlist.add(tobj);
				}
				maptaskqueue.add(newmaptask);
			}
			
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
					
			Iterator<Job> itr = jobqueue.iterator();
			Job item=null;
			while(itr.hasNext())
			{
				item = itr.next();
				if(item.jid == request.getJobId())
				{
					response.setTotalMapTasks(item.numofmaptasks).setTotalReduceTasks(item.numofreducetasks);
					
					//Count number of map tasks started of job
					int mapcount=0;
					ArrayList<MapTask> maptasklist = new ArrayList<MapTask>();
					for(MapTask mapitem : maptaskqueue)
					{
						if(mapitem.jid == item.jid && mapitem.taken)
						{
							mapcount++;
							maptasklist.add(mapitem);
						}
					}
					
					//Count number of reduce tasks started of job
					int reducecount = 0;
					for(ReduceTask reduceitem : reducetaskqueue)
					{
						if(reduceitem.jid == item.jid && reduceitem.taken)
							reducecount++;
					}
					
					response.setNumMapTasksStarted(mapcount).setNumReduceTasksStarted(reducecount);

					if(item.completedmaptasks == item.numofmaptasks)
					{
						if(item.reduceStage == false)
						{
							//select random tasktrackers
							Collections.shuffle(tasktrackerlist);
							int uplimit = (int)Math.ceil(((double)item.numofmaptasks)/((double)item.numofreducetasks));

							int taskcount = 0;
							//generate reduce tasks
							for(int i=0;i<item.numofreducetasks;i++)
							{
								int random = (int)((Math.random() * 10000) + 1); //get random job id
								while(findTaskInQueue(reducetaskqueue,random))
								{
									random++;
								}

								ReduceTask newreducetask = new ReduceTask(random,item.jid,item.reducerName,item.outputFile + "_" + Integer.toString(item.jid) + "_" + Integer.toString(random));
								for(int j=0;j<uplimit && taskcount<item.numofmaptasks;j++)
								{
									newreducetask.filenamelist.add("job_"+Integer.toString(item.jid) + "_map_" + Integer.toString(maptasklist.get(j).tid));
								}
								reducetaskqueue.add(newreducetask);									
							}
							item.reduceStage = true;
						}

						//check if job completed
						if(item.completedreducetasks == item.numofreducetasks)
						{
							response.setJobDone(true);
							itr.remove();
						}
					}
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
		maprformat.HeartBeatResponse.Builder response = maprformat.HeartBeatResponse.newBuilder();
		try
		{
			maprformat.HeartBeatRequest request = maprformat.HeartBeatRequest.parseFrom(inpdata);
			if(!tasktrackerlist.contains(request.getTaskTrackerId()))
			{	
				tasktrackerlist.add(request.getTaskTrackerId());
			}
			
			//TODO: map task status update info
			for(int i=0;i<request.getMapStatusCount();i++)
			{
				if(request.getMapStatus(i).hasTaskCompleted())
				{
					
					for(int j=0;j<jobqueue.size();j++)
					{
						if(jobqueue.get(j).jid == request.getMapStatus(i).getJobId())
						{
							//Tell job that a maptask has been completed
							jobqueue.get(j).completedmaptasks++;
						}
					}
					
					Iterator<MapTask> itr = maptaskqueue.iterator();
					MapTask temp=null;
					while(itr.hasNext())
					{
						temp=itr.next();
						if(temp.tid == request.getMapStatus(i).getTaskId())
						{
							itr.remove();
						}
					}
				}
			}
				
				
			//TODO: reduce task status update info
			for(int i=0;i<request.getReduceStatusCount();i++)
			{
				for(int j=0;j<jobqueue.size();j++)
				{
					if(jobqueue.get(j).jid == request.getMapStatus(i).getJobId())
					{
						//Tell job that a reducetask has been completed
						jobqueue.get(j).completedreducetasks++;
					}
				}
				
				if(request.getReduceStatus(i).hasTaskCompleted())
				{
					Iterator<ReduceTask> itr = reducetaskqueue.iterator();
					ReduceTask temp=null;
					while(itr.hasNext())
					{
						temp=itr.next();
						if(temp.tid == request.getMapStatus(i).getTaskId())
						{
							itr.remove();
						}
					}
				}
			}
			
			//give map tasks
			for(int i=0;i<request.getNumMapSlotsFree();i++)
			{
				MapTask tempmtask = maptaskqueue.get(i);
				maprformat.BlockLocations.Builder bloc = maprformat.BlockLocations.newBuilder().setBlockNumber(tempmtask.inputchunkno);
				
				//Add DataNode Locations to block
				for(int j=0;j<tempmtask.Dnlist.size();j++)
				{
					maprformat.DataNodeLocation tloc = maprformat.DataNodeLocation.newBuilder().setIp(tempmtask.Dnlist.get(j).ip)
							.setPort(tempmtask.Dnlist.get(j).port).setName(tempmtask.Dnlist.get(j).serverName).build();
					bloc.addLocations(tloc);					
				}
				//Create Map task request. Send to TT. Only one chunk info needs to be added for a maptask.
				MapTaskInfo maptaskinforequest = MapTaskInfo.newBuilder().setJobId(tempmtask.jid)
						.setTaskId(tempmtask.tid).setMapName(tempmtask.mapName).addInputBlocks(bloc.build()).build();
				
				response.addMapTasks(maptaskinforequest);
			}
			
			// give reduce tasks 
			for(int i=0;i<request.getNumReduceSlotsFree();i++)
			{
				ReduceTask temprtask = reducetaskqueue.get(i);
				ReducerTaskInfo reducerequest = ReducerTaskInfo.newBuilder().setJobId(temprtask.jid)
						.setTaskId(temprtask.tid).setReducerName(temprtask.reducerName)
						.setOutputFile(temprtask.outputfile).addAllMapOutputFiles(temprtask.filenamelist).build();
			
				response.addReduceTasks(reducerequest);
			}
			
			response.setStatus(1);
			
		}catch(Exception e)
		{
			System.out.println("Error " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
		}
		
		return response.build().toByteArray();
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
		    		    
		    String temp = c.getMethod("map",String.class, String.class).invoke(c.newInstance(),"1234","45").toString();
		    System.out.println(temp);
		    
		    jarFile.close();
		}catch(Exception e)
        {
            System.out.println("Error " + e.toString());
            e.printStackTrace();
        }
	}
}
