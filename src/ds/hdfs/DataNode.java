//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import ds.hdfs.hdfsformat.*;
import ds.hdfs.IDataNode.*;

public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected String MyName;
    protected int MyID;

    public DataNode()
    {
        //Constructor
        this.MyChunksFile = "ChunkFile.txt";
    }

    public String FileTail(String f)
    {
        File file = null;
        file = new File(f);
        RandomAccessFile fileHandler = null;
        try 
        {
            fileHandler = new RandomAccessFile( file, "r" );
            long fileLength = fileHandler.length() - 1;
            StringBuilder sb = new StringBuilder();

            for(long filePointer = fileLength; filePointer != -1; filePointer--){
                fileHandler.seek( filePointer );
                int readByte = fileHandler.readByte();

                if( readByte == 0xA ) {
                    if( filePointer == fileLength ) {
                        continue;
                    }
                    break;

                } else if( readByte == 0xD ) {
                    if( filePointer == fileLength - 1 ) {
                        continue;
                    }
                    break;
                }

                sb.append( ( char ) readByte );
            }

            String lastLine = sb.reverse().toString();
            return lastLine;
        } 
        catch( java.io.FileNotFoundException e ) 
        {
            System.out.println("There is no file");     
            //e.printStackTrace();
            return null;
        } 
        catch( java.io.IOException e ) 
        {
            System.out.println("IOException file read ");       
            //e.printStackTrace();
            return null;
        } 
        finally 
        {
            if (fileHandler != null )
                try 
                {
                    fileHandler.close();
                } 
            catch (IOException e) 
            {
                /* ignore */
            }
        }
    }

    //Writes to a file with given string.. if First is true then no newline
    public static void appendtoFile(String Filename, String Line, boolean First)
    {
        BufferedWriter bw = null;

        try {
            // APPEND MODE SET HERE
            bw = new BufferedWriter(new FileWriter(Filename, true));
            if (First == false)
            {
                bw.newLine();
            }
            bw.write(Line);
            bw.flush();
        } 
        catch (IOException ioe) 
        {
            ioe.printStackTrace();
        } 
        finally 
        {                       // always close the file
            if (bw != null) try {
                bw.close();
            } catch (IOException ioe2) {
                // just ignore it
            }
        } // end try/catch/finally

    }

    public byte[] readBlock(byte[] Inp)
    {
        ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
        try
        {
            ReadBlockRequest deserObj = ReadBlockRequest.parseFrom(Inp);
            int chunkno = deserObj.getBlockNumber();
            String fname = Integer.toString(chunkno) + ".chunk";

            FileInputStream fis = new FileInputStream(fname);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = null;
            while ((line = br.readLine()) != null) 
            {
                System.out.println(line);
                response.addData(ByteString.copyFrom(line.getBytes()));
            }
            br.close();
        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    public byte[] writeBlock(byte[] Inp)
    {
        WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
        try
        {
            WriteBlockRequest deserObj = WriteBlockRequest.parseFrom(Inp);

            Future<Integer> future = null;

            if(deserObj.getBlockInfo().getLocationsCount() != 0) //Threading if another replication required
            {
                RunnableDemo nextcall = new RunnableDemo("Thread-1",deserObj);
                final ExecutorService service = Executors.newFixedThreadPool(2);
                future = service.submit(nextcall);    			
            }

            int chunkno = deserObj.getBlockInfo().getBlockNumber();
            System.out.println(chunkno);

            String fname = Integer.toString(chunkno) + ".chunk";

            File ftest = new File(fname);
            ftest.createNewFile(); //creates a new file only if one doesnt exist

            byte[] File;
            BufferedWriter out = new BufferedWriter(new FileWriter(fname, false));
            for(int i=0; i<deserObj.getDataCount();i++)
            {
                File = new byte[deserObj.getData(i).size()];
                deserObj.getData(i).copyTo(File,0);
                out.write(new String(File));
            }
            out.close();

            int isSuccess = 1;
            if(future != null)
            {
                isSuccess = future.get(); //wait here for thread to complete	
            }
            response.setStatus(isSuccess);
        }
        catch(Exception e)
        {
            System.out.println("Error at writeBlock " + e.toString());
            e.printStackTrace();
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    public void BlockReport() //Not tested
    {
        BlockReportRequest.Builder BlockReport = BlockReportRequest.newBuilder();
        BlockReport.setId(this.MyID);

        //Add IP and Port to DataNodeLocation 
        DataNodeLocation.Builder DNLoc = DataNodeLocation.newBuilder();
        DNLoc.setIp(this.MyIP);
        DNLoc.setPort(this.MyPort);
        DNLoc.setName(this.MyName);
        BlockReport.setLocation(DNLoc);

        //Add All the block numbers I posses
        FileReader fileReader;
        try{
            fileReader = new FileReader(new File(this.MyChunksFile));
        }catch(Exception e){
            System.out.println("Unable to open file in DataNode");
            return;
        }
        BufferedReader br = new BufferedReader(fileReader);
        String line = "";
        // if no more lines the readLine() returns null
        while (line != null) 
        {
            try{
                line = br.readLine();
                if(line == null)
                    return;
            }catch(Exception e){
                System.out.println("Unable to read the ChunkFile");
                return;
            }
            BlockReport.addBlockNumbers(Integer.parseInt(line)); //The ChunkFile contains the chunknumbers only     
        }
        byte[] Response;
        try{
            Response = this.NNStub.blockReport(BlockReport.build().toByteArray());
        }catch(Exception e){
            System.out.println("Unable to send BlockReport to the NN");
            return;
        }
        BlockReportResponse response;
        try{
            response = BlockReportResponse.parseFrom(Response);
        }catch(Exception e){
            System.out.println("There is a problem in opening Proto in BlockReportResponse");
            return;
        }
        int Count = response.getStatusCount();
        boolean AllOk = true;
        for(int i=0; i<Count; i++)
        {
            if(response.getStatus(i) < 0)
                System.out.println("Huston, we have got a Status " + response.getStatus(i));
            else
            {
                System.out.println(response.getStatus(i) + " in Block number " + i);
                AllOk = false;
            }
        }
        if(AllOk == true)
            System.out.println("BlockReport Sent !!");
    }

    public void BindServer(String Name, String IP, int Port)
    {
        try
        {
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.getRegistry(Port);
            registry.bind(Name, stub);
            System.out.println("\nDataNode is ready\n");
        }
        catch(Exception e)
        {
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
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
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    public static void main(String args[]) throws InvalidProtocolBufferException 
    {
        //Define a Datanode Me
        DataNode Me = new DataNode();        

        /*
           ReadBlockRequest request = ReadBlockRequest.newBuilder().setBlockNumber(1003).build();
           ReadBlockResponse response = ReadBlockResponse.parseFrom(Me.readBlock(request.toByteArray()));
           System.out.println(response.toString());

           WriteBlockRequest request11 = WriteBlockRequest.newBuilder().setBlockInfo(BlockLocations.newBuilder()
           .setBlockNumber(1004).build()).addData(ByteString.copyFrom("line 1\n".getBytes())).build();
           WriteBlockResponse response11 = WriteBlockResponse.parseFrom(Me.writeBlock(request11.toByteArray()));
           System.out.println(response11.toString());
           */

        //Bind The DataNode Self
        String Config = Me.FileTail("dn_config.txt");
        String[] Split_Config = Config.split(";");
        Me.MyName = "DN" + Split_Config[0];
        Me.MyID = Integer.parseInt(Split_Config[0]);
        Me.MyIP = Split_Config[1];
        Me.MyPort = Integer.parseInt(Split_Config[2]);        
        Me.BindServer(Me.MyName, Me.MyIP, Me.MyPort); //Name, IP, Port

        /*
           WriteBlockRequest request12 = WriteBlockRequest.newBuilder().setBlockInfo(BlockLocations.newBuilder()
           .setBlockNumber(1005)
           .addLocations(DataNodeLocation.newBuilder().setIp(Me.MyIP).setPort(Me.MyPort).build())
           .addLocations(DataNodeLocation.newBuilder().setIp("4.5.6.7").setPort(Me.MyPort).build()).build())
           .addData(ByteString.copyFrom("line 1\n".getBytes())).build();

           WriteBlockResponse response12 = WriteBlockResponse.parseFrom(Me.writeBlock(request12.toByteArray()));
           System.out.println(response12.toString());
           */

        //Get The NameNode
        String NNConfig = Me.FileTail("nn_details.txt");
        String[] NNSplit_Config = NNConfig.split(";");
        Me.NNStub = Me.GetNNStub(NNSplit_Config[0], NNSplit_Config[1], Integer.parseInt(NNSplit_Config[2])); // Name, IP, Port

        while(true)
        {
            Me.BlockReport();
            try{
                TimeUnit.SECONDS.sleep(10); //Wait for 10 Seconds
            }catch(Exception e){
                System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
                //e.printStackTrace();
            }
        }
    }
}

class RunnableDemo implements Callable<Integer>
{
    private Thread t;
    private String threadName;
    WriteBlockRequest passobj;

    RunnableDemo(String name, WriteBlockRequest req) 
    {
        threadName = name;
        passobj = req;
        System.out.println("Creating " +  threadName );
    }		

    public Integer call() 
    {
        try 
        {
            System.out.println("Running " +  threadName );
            System.out.println(passobj.toString());
            DataNodeLocation targetdn = passobj.getBlockInfo().getLocations(0);

            //remove a location from the locations list
            BlockLocations temp = passobj.getBlockInfo().toBuilder().removeLocations(0).build();
            passobj = passobj.toBuilder().clearBlockInfo().build();
            passobj = passobj.toBuilder().setBlockInfo(temp).build();

            System.out.println(targetdn.toString());
            System.out.println(passobj.toString());

            //TODO: call writeblock rpc of targetdn and send deserObj
            Registry registry = LocateRegistry.getRegistry(targetdn.getIp(),targetdn.getPort());
            //IDataNode stub = (IDataNode)registry.lookup("Server");
            IDataNode stub = (IDataNode)registry.lookup(targetdn.getName());
            WriteBlockResponse response = WriteBlockResponse.parseFrom(stub.writeBlock(passobj.toByteArray()));

            System.out.println("Thread " +  threadName + " exiting.");
            if(response.getStatus() < 0)
                return -1;
            else
                return 1;
        }
        catch (Exception e) 
        {
            System.out.println("Thread " +  threadName + " interrupted.");
            e.printStackTrace();

            return -1;
        }	
    }
}
