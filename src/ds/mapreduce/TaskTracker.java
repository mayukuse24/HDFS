//Written By Shaleen Garg
package ds.mapreduce;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.*;
/*
   import java.util.concurrent.Callable;
   import java.util.concurrent.ExecutorService;
   import java.util.concurrent.Executors;
   import java.util.concurrent.Future;
   import java.util.concurrent.TimeUnit;
   import java.util.concurrent.ThreadPoolExecutor;
   import java.util.concurrent.LinkedBlockingQueue;
   import java.util.concurrent.TimeUnit;
   */

import java.io.*;
import java.nio.charset.Charset;

import ds.mapreduce.maprformat.*;
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
    protected Future<Integer>[] MapFuture;
    protected Future[] ReduceFuture;

    public TaskTracker(int id, int mapthreads, int reducethreads)
    {
        this.MyID = id;
        this.MapThreads = mapthreads;
        this.ReduceThreads = reducethreads;
        //Defining Individual threadpools
        this.MapPool = Executors.newFixedThreadPool(this.MapThreads);
        this.ReducePool = Executors.newFixedThreadPool(this.ReduceThreads);
        //MapFuture = new Future<Integer>[this.MapThreads];
        //ReduceFuture = new Future[this.ReduceThreads];
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

    
        MapperFunc callmap = new MapperFunc();
        ReducerFunc callreduce = new ReducerFunc();
        Future<Integer> future = TT.MapPool.submit(callmap);
        System.out.println("Active MapTasks = " + TT.MapPool);
        Future<Integer> f = TT.ReducePool.submit(callreduce);
        System.out.println("Active ReduceTasks = " + TT.ReducePool);

        /*
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
            HeartBeatRequest.Builder HBR = HeartBeatRequest.newBuilder();
            HBR.setTaskTrackerId(TT.MyID);
            HBR.setNumMapSlotsFree(); //Fill her up
            HBR.setNumReduceSlotsFree(); //Fill her up

            //Wait for 1 sec
            try{
                TimeUnit.SECONDS.sleep(1); //Wait for 1 Seconds
            }catch(Exception e){
                System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
            }

        }
        */

    }
}

//This function will load the mapper function from the jar; perform it -
//And write it to a file job_<jobid>_map_<taskid>
class MapperFunc implements Callable<Integer> 
{
    MapperFunc()
    {
        //Initializer with the needed inputs
    }

    //This is the function which will be called everytime MapperFunc is called
    public Integer call()
    {
        System.out.println("MapperFunc");
        try{
            TimeUnit.SECONDS.sleep(15); //Wait for 15 Seconds
        }catch(Exception e){
            System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
        }
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
            TimeUnit.SECONDS.sleep(15); //Wait for 15 Seconds
        }catch(Exception e){
            System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
        }
        return 1;
    }
}
