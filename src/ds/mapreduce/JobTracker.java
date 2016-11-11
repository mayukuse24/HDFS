package ds.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
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
import ds.hdfs.hdfsformat.BlockLocationRequest;
import ds.hdfs.hdfsformat.BlockLocationResponse;
import ds.hdfs.hdfsformat.BlockLocations;
import ds.hdfs.hdfsformat.CloseFileRequest;
import ds.hdfs.hdfsformat.CloseFileResponse;
import ds.hdfs.hdfsformat.HeartBeatResponse;
import ds.hdfs.hdfsformat.OpenFileRequest;
import ds.hdfs.hdfsformat.OpenFileResponse;
import ds.hdfs.*;

public class JobTracker implements IJobTracker{

	
	Client hdfsclient;
	String ip,name;
	int port;
	protected Registry serverRegistry;
	
	public JobTracker(String addr,int p, String nn)
	{
		ip = addr;
		port = p;
		name = nn;
	}
	
	public class Job
	{
		int jid;
		int numofreducetasks,numofmaptasks;
		int completedreducetasks,completedmaptasks;
		int inputfilehandle; //TODO:required to close hdfs file in end
		String mapName,reducerName,inputFile,outputFile;
		boolean reduceStage;
		ArrayList<Integer> maptasklist;
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
			maptasklist = new ArrayList<Integer>();
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
    
    private String printTaskId(ArrayList<? extends TaskData> qt)
    {
    	String output = "[";
    	for(TaskData item : qt)
    	{
    		output += " " + Integer.toString(item.tid);
    	}
    	return output + "]";
    }
    
    private String printList(ArrayList<String> qt)
    {
    	String output = "[";
    	for(String item : qt)
    	{
    		output += " " + item;
    	}
    	return output + "]";
    }
    private boolean checkChunksForEmptyLocations(BlockLocationResponse blocresponse)
    {
    	//test if all chunks got DN locations
		for(int k=0;k<blocresponse.getBlockLocationsCount();k++)
		{
			if(blocresponse.getBlockLocations(k).getLocationsCount() == 0)
			{
				return false;
			}
		}
		
		return true;
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
			
			// Create and add new job
			Job newjob = new Job(random,openfileresponse.getBlockNumsCount(),request.getNumReduceTasks(),request.getMapName(),request.getReducerName(),
					request.getInputFile(),request.getOutputFile(),openfileresponse.getHandle());

			jobqueue.add(newjob);

			BlockLocationResponse blocresponse;
			while(true)
			{
				BlockLocationRequest.Builder blocrequest = BlockLocationRequest.newBuilder();
				//add chunks to blockrequest for DN
				for(int i=0;i<openfileresponse.getBlockNumsCount();i++)
				{
					blocrequest.addBlockNums(openfileresponse.getBlockNums(i));
				}

				//get DNs for chunks
				blocresponse = BlockLocationResponse.parseFrom(this.hdfsclient.NNStub
						.getBlockLocations(blocrequest.build().toByteArray()));
				
				if(blocresponse.getStatus() == 1 && checkChunksForEmptyLocations(blocresponse)) //NameNode did not find DNs yet. Retry to see if NN updated
				{
					break;
				}
				else
				{
					//got no data locations for some chunk. Wait for sometime
					System.out.println("Found chunk with empty Location set. Trying again");
					TimeUnit.SECONDS.sleep(3);
				}
			}
			
			System.out.println(blocresponse.toString());

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
			
			//Close hdfs file opened to read chunk nos
			CloseFileRequest closefilerequest = CloseFileRequest.newBuilder().setHandle(openfileresponse.getHandle()).build();
			CloseFileResponse closefileresponse = CloseFileResponse.parseFrom(this.hdfsclient.NNStub.closeFile(closefilerequest.toByteArray()));
			
			System.out.println("maptaskqueue size " + maptaskqueue.size());
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
					
					//Count number of map tasks running of job
					int mapcount=0;
					for(MapTask mapitem : maptaskqueue)
					{
						if(mapitem.jid == item.jid && mapitem.taken)
						{
							mapcount++;
							item.maptasklist.add(mapitem.tid);
						}
					}
					
					//Count number of reduce tasks running of job
					int reducecount = 0;
					for(ReduceTask reduceitem : reducetaskqueue)
					{
						if(reduceitem.jid == item.jid && reduceitem.taken)
							reducecount++;
					}
					
					System.out.println("Current status of job " + Integer.toString(item.jid) + " maptasks= " + Integer.toString(mapcount) + "-" 
					+ Integer.toString(item.completedmaptasks)+ "/" + Integer.toString(item.numofmaptasks) + printTaskId(maptaskqueue));
					
					System.out.println("Current status of job " + Integer.toString(item.jid) + " reducetasks= " + Integer.toString(reducecount) + "-" 
							+ Integer.toString(item.completedreducetasks)+ "/" + Integer.toString(item.numofreducetasks) + printTaskId(reducetaskqueue));
					
					response.setNumMapTasksStarted(mapcount+item.completedmaptasks).setNumReduceTasksStarted(reducecount+item.completedreducetasks);

					if(item.completedmaptasks == item.numofmaptasks)
					{
						if(item.reduceStage == false)
						{
							//select random tasktrackers
							Collections.shuffle(tasktrackerlist);
							int uplimit = (int)Math.ceil(((double)item.numofmaptasks)/((double)item.numofreducetasks));
							System.out.println("uplimit = " + Integer.toString(uplimit));
							
							int taskcount = 0; //counts number of reduce tasks generated so far
							int mapcounter = 0; //counts number of map tasks changed to reduce
							
							//generate reduce tasks
							for(int i=0;i<item.numofreducetasks;i++)
							{
								int random = (int)((Math.random() * 10000) + 1); //get random reduce task id
								while(findTaskInQueue(reducetaskqueue,random))
								{
									random++;
								}

								ReduceTask newreducetask = new ReduceTask(random,item.jid,item.reducerName,item.outputFile + "_" + Integer.toString(item.jid) + "_" + Integer.toString(random));
								for(int j=0;j<uplimit && taskcount<item.numofmaptasks && mapcounter<item.maptasklist.size();j++,mapcounter++)
								{
									newreducetask.filenamelist.add("job_"+Integer.toString(item.jid) + "_map_" + Integer.toString(item.maptasklist.get(mapcounter)));
								}
								taskcount++;
								reducetaskqueue.add(newreducetask);									
							}
							System.out.println("Generated reduce tasks " + Integer.toString(taskcount) + " Out of map tasks " + Integer.toString(mapcounter));
							item.reduceStage = true;
						}

						//check if job completed
						if(item.completedreducetasks == item.numofreducetasks)
						{
							response.setJobDone(true);
							System.out.println("Job " + Integer.toString(item.jid) + " Completed");
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
				System.out.println("Found TT " +Integer.toString(request.getTaskTrackerId()));
			}
			
			//TODO: map task status update info
			for(int i=0;i<request.getMapStatusCount();i++)
			{
				if(request.getMapStatus(i).getTaskCompleted() && findTaskInQueue(maptaskqueue,request.getMapStatus(i).getTaskId()))
				{
					for(int j=0;j<jobqueue.size();j++)
					{
						if(jobqueue.get(j).jid == request.getMapStatus(i).getJobId())
						{
							//Tell job that a maptask has been completed
							jobqueue.get(j).completedmaptasks++;
						}
					}
					
					//remove maptask from queue
					Iterator<MapTask> itr = maptaskqueue.iterator();
					MapTask temp=null;
					while(itr.hasNext())
					{
						temp=itr.next();
						if(temp.tid == request.getMapStatus(i).getTaskId())
						{
							itr.remove();
							System.out.print("maptask " + Integer.toString(temp.tid) + " removed");
						}
					}
					System.out.println("Found completed task " + Integer.toString(request.getMapStatus(i).getTaskId()));
				}
			}
				
			//TODO: reduce task status update info
			for(int i=0;i<request.getReduceStatusCount();i++)
			{
				if(request.getReduceStatus(i).getTaskCompleted() && findTaskInQueue(reducetaskqueue,request.getReduceStatus(i).getTaskId()))
				{
					
					for(int j=0;j<jobqueue.size();j++)
					{
						if(jobqueue.get(j).jid == request.getReduceStatus(i).getJobId())
						{
							//Tell job that a reducetask has been completed
							jobqueue.get(j).completedreducetasks++;
						}
					}
				
					Iterator<ReduceTask> itr = reducetaskqueue.iterator();
					ReduceTask temp=null;
					while(itr.hasNext())
					{
						temp=itr.next();
						if(temp.tid == request.getReduceStatus(i).getTaskId())
						{
							itr.remove();
							System.out.print("reducetask " + Integer.toString(temp.tid) + " removed");
						}
					}
				}
			}
			
			int givenmaptasks = 0;
			//give map tasks
			for(int i=0;givenmaptasks<request.getNumMapSlotsFree() && i<maptaskqueue.size();i++)
			{
				MapTask tempmtask = maptaskqueue.get(i);
				if(!tempmtask.taken)
				{
					maprformat.BlockLocations.Builder bloc = maprformat.BlockLocations.newBuilder().setBlockNumber(tempmtask.inputchunkno);
					
					//Add DataNode Locations to block
					for(int j=0;j<tempmtask.Dnlist.size();j++) //TODO: loop not required, only one block per maptask
					{
						maprformat.DataNodeLocation tloc = maprformat.DataNodeLocation.newBuilder().setIp(tempmtask.Dnlist.get(j).ip)
								.setPort(tempmtask.Dnlist.get(j).port).setName(tempmtask.Dnlist.get(j).serverName).build();
						bloc.addLocations(tloc);					
					}
					//Create Map task request. Send to TT. Only one chunk info needs to be added for a maptask.
					MapTaskInfo maptaskinforequest = MapTaskInfo.newBuilder().setJobId(tempmtask.jid)
							.setTaskId(tempmtask.tid).setMapName(tempmtask.mapName).addInputBlocks(bloc.build()).build();
					
					response.addMapTasks(maptaskinforequest);
					tempmtask.taken = true;
					givenmaptasks++;
				}
			}
			
			int givenreducetasks = 0;
			// give reduce tasks 
			for(int i=0;givenreducetasks<request.getNumReduceSlotsFree() && i<reducetaskqueue.size();i++)
			{
				ReduceTask temprtask = reducetaskqueue.get(i);
				if(!temprtask.taken)
				{
					ReducerTaskInfo reducerequest = ReducerTaskInfo.newBuilder().setJobId(temprtask.jid)
							.setTaskId(temprtask.tid).setReducerName(temprtask.reducerName)
							.setOutputFile(temprtask.outputfile).addAllMapOutputFiles(temprtask.filenamelist).build();
				
					response.addReduceTasks(reducerequest);
					temprtask.taken = true;
					givenreducetasks++;
					System.out.println("Assigning reduce task " + Integer.toString(temprtask.tid) + " to " + Integer.toString(request.getTaskTrackerId()) + " for files " + printList(temprtask.filenamelist));;
				}
			}
			
			if(givenreducetasks == 0 && givenmaptasks == 0)
			{
				response.setStatus(0); //no tasks assigned
			}
			else
			{
				System.out.println(Integer.toString(request.getTaskTrackerId()) + " MapTasks sent = " +Integer.toString(givenmaptasks) + " Reducetasks sent " + Integer.toString(givenreducetasks));
				response.setStatus(1);
			}
				
			
		}catch(Exception e)
		{
			System.out.println("Error " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
	
	public static void main(String[] args) throws NumberFormatException, IOException
	{
		FileInputStream fis = new FileInputStream("jt_details.txt");
		
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		JobTracker JT = null;
		
		String line = null;
		while ((line = br.readLine()) != null) 
		{
			System.out.println(line);
			String[] Split_Config = line.split(";");
			if("JT".equals(Split_Config[0]))
			{
				JT = new JobTracker(Split_Config[1],Integer.parseInt(Split_Config[2]),Split_Config[0]);
				break;
			}
		}
		br.close();
		
		// To read config file and Connect to NameNode
        //Intitalize the Client       
		
        
        if(JT == null)
		{
			System.out.println("Failed to configure JT. Check jt_details.txt file");
			System.exit(-1);
		}
        
        JT.hdfsclient = new Client();
        System.out.println("Acquiring NameNode stub");

        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
        String Config = Client.FileTail("nn_details.txt");
        String[] Split_Config = Config.split(";");
        JT.hdfsclient.NNStub = JT.hdfsclient.GetNNStub(Split_Config[0], Split_Config[1], Integer.parseInt(Split_Config[2]));
        
        
        // JT stubs to rmiregistry
        System.setProperty("java.rmi.server.hostname", JT.ip);
		System.setProperty("java.security.policy","test.policy");

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        		
		//final NetworkTest serverObj = new NetworkTest(args[0]);
		try {
			IJobTracker stub = (IJobTracker) UnicastRemoteObject.exportObject(JT, 0);

			// Bind the remote object's stub in the registry
			JT.serverRegistry = LocateRegistry.getRegistry(JT.port);
			JT.serverRegistry.rebind(JT.name, stub);

			System.err.println(JT.name + " ready");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString() + " Failed to start server");
			e.printStackTrace();
		}
		
		ArrayList<Integer> temp = new ArrayList<Integer>();
		System.out.println(temp);
        
	}
}
