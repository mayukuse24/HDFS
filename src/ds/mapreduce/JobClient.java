//Written By Shaleen Garg
package ds.mapreduce;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
//import ds.hdfs.hdfsformat.*;

import ds.mapreduce.maprformat.*;
import com.google.protobuf.ByteString; 
import ds.hdfs.Client;

public class JobClient
{
    protected String MapName;
    protected String ReducerName;
    protected String InputFile; 
    protected String OutputFile;
    protected int NumReducers;
    protected IJobTracker JTStub;

    public JobClient(String mapname, String reducername, String inputfile, String outputfile, int numreducers)
    {
        this.MapName = mapname;
        this.ReducerName = reducername;
        this.InputFile = inputfile;
        this.OutputFile = outputfile;
        this.NumReducers = numreducers;
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

    public static void main(String[] args)
    {
        if(args.length != 5) //Check if it was called correctly
        {
            System.out.println("invoke in format $java ds.mapreduce.JobClient <mapName> <reducerName> <InputFile> <OutputFile> <numReducers>");
            return ;
        }
        JobClient JC = new JobClient(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]));
        Client HC = new Client();
        //This is to get the NN for the client
        String Config = HC.FileTail("nn_details.txt");
        String[] Split_Config = Config.split(";");
        HC.NNStub = HC.GetNNStub(Split_Config[0], Split_Config[1], Integer.parseInt(Split_Config[2]));
        System.out.println("Got NNStub from the hdfs");
        HC.PutFile(JC.InputFile); //Put the inputfile in HDFS


        String Config_JT = HC.FileTail("jt_details.txt");
        String[] SC = Config_JT.split(";");
        JC.JTStub = JC.GetJTStub(SC[0], SC[1], Integer.parseInt(SC[2])); //Name, IP, Port

        JobSubmitRequest.Builder JobSubmitReq = JobSubmitRequest.newBuilder();
        JobSubmitReq.setMapName(JC.MapName);
        JobSubmitReq.setReducerName(JC.ReducerName);
        JobSubmitReq.setInputFile(JC.InputFile);
        JobSubmitReq.setOutputFile(JC.OutputFile);
        JobSubmitReq.setNumReduceTasks(JC.NumReducers);

        //Submitting a Job
        byte[] Resp;        
        try{
            Resp = JC.JTStub.jobSubmit(JobSubmitReq.build().toByteArray());
        }catch(Exception e){
            System.out.println("Remote Error while submitting Job");
            return;
        }
        JobSubmitResponse JSR;
        try{
            JSR = JobSubmitResponse.parseFrom(Resp);
        }catch(Exception e){
            System.out.println("Error while parsing JobsubmitResponse proto");
            return;
        }
        if(JSR.getStatus() < 0) //Check if Status is ok
        {
            System.out.println("Huston, We have JobSubmitResponse Status = " + JSR.getStatus());
            return;
        }

        //JobStatus Request till JobDone
        JobStatusRequest.Builder JobStatusReq = JobStatusRequest.newBuilder();
        JobStatusReq.setJobId(JSR.getJobId());
        Resp = null; //Reset the Resp
        boolean JobDone = false;
        while(JobDone == false)
        {
            try{
                Resp = JC.JTStub.getJobStatus(JobStatusReq.build().toByteArray());
            }catch(Exception e){
                System.out.println("Remote Error while getting JobStatus response");
                return;
            }
            JobStatusResponse JobStatusResp;
            try{
                JobStatusResp = JobStatusResponse.parseFrom(Resp);
            }catch(Exception e){
                System.out.println("Error while parsing JobStatusResponse proto");
                return;
            }
            if(JobStatusResp.getStatus() < 0)
            {
                System.out.println("Huston, We have JobStatusResponse Status = " + JobStatusResp.getStatus());
                return;
            }
            JobDone = JobStatusResp.getJobDone();
            System.out.println("Total MapTasks = " + JobStatusResp.getTotalMapTasks());
            System.out.println("Num of  MapTasks Started = " + JobStatusResp.getNumMapTasksStarted());
            System.out.println("Total ReduceTasks = " + JobStatusResp.getTotalReduceTasks());
            System.out.println("Num of  ReduceTasks Started = " + JobStatusResp.getNumReduceTasksStarted());
            System.out.println("##############################################################################");
            Resp = null; //Reset the Resp
        }
        System.out.println("MapReduce Job Complete !!!");
    }
}
