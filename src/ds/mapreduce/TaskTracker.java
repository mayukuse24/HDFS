//Written By Shaleen Garg
package ds.mapreduce;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import ds.mapreduce.maprformat.*;
import ds.hdfs.hdfsformat.*;
import com.google.protobuf.ByteString; 
import com.google.protobuf.InvalidProtocolBufferException;
import ds.hdfs.Client;
import ds.hdfs.INameNode;

public class TaskTracker
{
    protected int MapThreads;
    protected int ReduceThreads;
    protected int MyID;
    protected IJobTracker JTStub;
    protected INameNode NNStub;
    final protected ExecutorService MapPool;
    final protected ExecutorService ReducePool;
    protected List<Maptasks> MapTasksList = new ArrayList<Maptasks>();
    protected List<Reducetasks> ReduceTasksList = new ArrayList<Reducetasks>();

    public TaskTracker(int id, int mapthreads, int reducethreads)
    {
        this.MyID = id;
        this.MapThreads = mapthreads;
        this.ReduceThreads = reducethreads;

        //Defining Individual threadpools
        this.MapPool = Executors.newFixedThreadPool(this.MapThreads);
        this.ReducePool = Executors.newFixedThreadPool(this.ReduceThreads);
    }

    public IJobTracker GetJTStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IJobTracker stub = (IJobTracker) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                System.out.println("Still waiting for JobTracker");
                continue;
            }
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    public static void main(String[] args)
    {
        String Config = Client.FileTail("TT_details.txt");
        String[] SC = Config.split(";");
        TaskTracker TT = new TaskTracker(Integer.parseInt(SC[0]), Integer.parseInt(SC[1]), Integer.parseInt(SC[2]));

        String Config_JT = Client.FileTail("jt_details.txt");
        String[] Sc = Config_JT.split(";");
        TT.JTStub = TT.GetJTStub(Sc[0], Sc[1], Integer.parseInt(Sc[2])); //Name, IP, Port

        //Get The NameNode
        String NNConfig = Client.FileTail("nn_details.txt");
        String[] NNSplit_Config = NNConfig.split(";");
        TT.NNStub = TT.GetNNStub(NNSplit_Config[0], NNSplit_Config[1], Integer.parseInt(NNSplit_Config[2])); // Name, IP, Port

        //Send Heartbeat to the JT
        while(true)
        {
            maprformat.HeartBeatRequest.Builder HBR = maprformat.HeartBeatRequest.newBuilder();
            HBR.setTaskTrackerId(TT.MyID);
            HBR.setNumMapSlotsFree(TT.MapThreads - ((ThreadPoolExecutor)TT.MapPool).getActiveCount());
            HBR.setNumReduceSlotsFree(TT.ReduceThreads - ((ThreadPoolExecutor)TT.ReducePool).getActiveCount());

            for(int i=0; i<TT.MapTasksList.size(); i++)
            {
                MapTaskStatus.Builder MPS = MapTaskStatus.newBuilder();
                MPS.setJobId(TT.MapTasksList.get(i).JobID);
                MPS.setTaskId(TT.MapTasksList.get(i).TaskID);
                MPS.setTaskCompleted(TT.MapTasksList.get(i).TaskComplete);
                MPS.setMapOutputFile(TT.MapTasksList.get(i).OutputFile);
                HBR.addMapStatus(MPS.build());
            }

            for(int i=0; i<TT.ReduceTasksList.size(); i++)
            {
                ReduceTaskStatus.Builder RPS = ReduceTaskStatus.newBuilder();
                RPS.setJobId(TT.ReduceTasksList.get(i).JobID);
                RPS.setTaskId(TT.ReduceTasksList.get(i).TaskID);
                RPS.setTaskCompleted(TT.ReduceTasksList.get(i).TaskComplete);
                HBR.addReduceStatus(RPS.build());
            }

            //Remove done elements from ReduceTaskList and MapTasksList
            for(int i=0; i<TT.ReduceTasksList.size();)
            {
                if(TT.ReduceTasksList.get(i).TaskComplete == true)
                {
                    TT.ReduceTasksList.remove(i);
                }
                else
                    i++;
            }

            for(int i=0; i<TT.MapTasksList.size();)
            {
                if(TT.MapTasksList.get(i).TaskComplete == true)
                {
                    TT.MapTasksList.remove(i);
                }
                else
                    i++;
            }

            byte[] R;
            try{
                R = TT.JTStub.heartBeat(HBR.build().toByteArray());
            }catch(Exception e){
                System.out.println("Unable to send HeartBeat to the JT");
                return;
            }

            maprformat.HeartBeatResponse HeartBeatResp;
            try{
                HeartBeatResp = maprformat.HeartBeatResponse.parseFrom(R);
            }catch(Exception e){
                System.out.println("There is some problem while decoding the HeartBeatResponse in proto");
                return;
            }
            if(HeartBeatResp.getStatus() < 0)
            {
                System.out.println("Huston, We have HeartBeatResponse Status = " + HeartBeatResp.getStatus());
                return;
            }
            for(int i=0; i<HeartBeatResp.getMapTasksCount(); i++)
            {
                Maptasks MT = new Maptasks();
                MT.TaskComplete = false;
                MT.JobID = HeartBeatResp.getMapTasks(i).getJobId();
                MT.TaskID = HeartBeatResp.getMapTasks(i).getTaskId();
                MT.OutputFile = "job_" + Integer.toString(MT.JobID) + "_map_" + Integer.toString(MT.TaskID);
                MT.MapName = HeartBeatResp.getMapTasks(i).getMapName();
                MT.BlockNo = HeartBeatResp.getMapTasks(i).getInputBlocks(0).getBlockNumber();
                MT.DNName = HeartBeatResp.getMapTasks(i).getInputBlocks(0).getLocations(0).getName();
                MT.DNPort = HeartBeatResp.getMapTasks(i).getInputBlocks(0).getLocations(0).getPort();
                MT.DNIP = HeartBeatResp.getMapTasks(i).getInputBlocks(0).getLocations(0).getIp();
                MapperFunc CallMap = new MapperFunc(MT);
                MT.future = TT.MapPool.submit(CallMap);
                TT.MapTasksList.add(MT);
               // TT.MapTasksList.get((TT.MapTasksList.size()-1)).future.isDone();
            }

            for(int i=0; i<HeartBeatResp.getReduceTasksCount(); i++)
            {
                //Spawn the Reduce Tasks
                Reducetasks RT = new Reducetasks();
                RT.TaskComplete = false;
                RT.JobID = HeartBeatResp.getReduceTasks(i).getJobId();
                RT.TaskID = HeartBeatResp.getReduceTasks(i).getTaskId();
            }

            //Wait for 1 sec
            try{
                TimeUnit.SECONDS.sleep(1); //Wait for 1 Seconds
            }catch(Exception e){
                System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
            }
        }
    }
}

class Maptasks
{
    public int JobID;
    public int TaskID;
    public boolean TaskComplete;
    public String OutputFile;
    public String MapName;
    public Future <Integer> future;

    public int BlockNo;
    public String DNName;
    public int DNPort;
    public String DNIP;

    public Maptasks()
    {
    }
}

class Reducetasks
{
    public int JobID;
    public int TaskID;
    public boolean TaskComplete;
    public Future <Integer> future;

    public Reducetasks()
    {
    }
}

//This function will load the mapper function from the jar; perform it -
//And write it to a file job_<jobid>_map_<taskid>
//STILL TO BE COMPLETED
class MapperFunc implements Callable<Integer>
{
    Maptasks MT;
    MapperFunc(Maptasks inp)
    {
        //Initializer with the needed inputs
        this.MT = inp;
    }
    //This is the function which will be called everytime MapperFunc is called
    public Integer call() throws IOException, ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, InstantiationException
    {
        //Get file from HDFS
        Client TTC = new Client();
        TTC.DNStub = TTC.GetDNStub(MT.DNName, MT.DNIP, MT.DNPort); //Name, IP, Port
        ReadBlockRequest.Builder ReadBlockReq = ReadBlockRequest.newBuilder();
        ReadBlockReq.setBlockNumber(MT.BlockNo);
        byte[] Res;
        try{
            System.out.println("Waiting for the DNStub ReadBlockRequest response");
            Res = TTC.DNStub.readBlock(ReadBlockReq.build().toByteArray());
            System.out.println("Got the DNStub ReadBlockRequest response");
        }catch(Exception e){
            System.out.println("Unable to send ReadBLock request from MapperFunc to DN");
            return -1;
        }
        ReadBlockResponse BlockResp;
        try{
            BlockResp = ReadBlockResponse.parseFrom(Res);
        }catch(Exception e){
            System.out.println("Unable to decode the ReadBlockResponse proto in MapperFunc");
            return -1;
        }
        if(BlockResp.getStatus() < 0)
        {
            System.out.println("Huston We have ReadBlockResponse Status = " + BlockResp.getStatus());
            return -1;
        }

        //Get Jar
        String PathToJar = Paths.get("").toAbsolutePath().toString() + "/jarnewtest.jar";
        JarFile jarfile = new JarFile(PathToJar);
        URL[] urls = { new URL("jar:file:" + PathToJar + "!/")};
        URLClassLoader cl = URLClassLoader.newInstance(urls);
        Class<?> c = cl.loadClass(MT.MapName);

        //Get the regex from REGEX.txt file 
        String Regex = Client.FileTail("REGEX.txt");
        //Send the Lines of the block to the Jar and write the output to the Outputfile
        try{
            FileOutputStream fos = new FileOutputStream(this.MT.OutputFile, true);
            for(ByteString A : BlockResp.getDataList())
            {
                String S = A.toStringUtf8();
                String Result = c.getMethod("map", String.class).invoke(c.newInstance(), S, Regex).toString();
                fos.write(Result.getBytes());
            }
            fos.close();
        }catch(Exception e){
            System.out.println("IOError while writing to the file in MapperFunc");
            return -1;
        }
        //Now to write this file back to the hdfs
        TTC.PutFile(this.MT.OutputFile);
        return 1;
    }
}

class ReducerFunc implements Callable<Integer> 
{
    ReducerFunc()
    {
        //Initializer with the needed inputs
    }

    //This is the function which will be called everytime ReducerFunc is called
    public Integer call()
    {
        System.out.println("ReducerFunction");
        try{
            TimeUnit.SECONDS.sleep(1); //Wait for 15 Seconds
        }catch(Exception e){
            System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
        }
        return 1;
    }
}
