//Program written by Shaleen Garg
package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import ds.hdfs.hdfsformat.*;
import com.google.protobuf.ByteString; 
//import ds.hdfs.INameNode;

public class Client
{
    //Variables Required
    protected INameNode NNStub; //Name Node stub
    protected IDataNode DNStub; //Data Node stub
    public Client()
    {
        //Constructor
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
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
                return stub;
            }catch(Exception e){
                continue;
            }
        }
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

    public void PutFile(String Filename) //Would like to try this
    {
        System.out.println("Going to put a file");
        File File = new File(Filename);
        BufferedInputStream bis;
        try{
            bis = new BufferedInputStream(new FileInputStream(File));
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }

        //Send the WriteRequest proto to the NameNode
        OpenFileRequest.Builder WriteReq = OpenFileRequest.newBuilder();
        WriteReq.setFileName(Filename);
        WriteReq.setForRead(false);  //For Writing
        byte[] Response;
        try{
            Response = this.NNStub.openFile(WriteReq.build().toByteArray());
        }catch(Exception e){
            System.out.println("Remote Exception while OpenFile Request!!");
            return;
        }
        OpenFileResponse response; 
        try{
            response = OpenFileResponse.parseFrom(Response);
        }catch(Exception e){
            System.out.println("Proto Problems in WriteRequest put");
            return;
        }
        if(response.getStatus() < 0)
        {
            System.out.println("Huston, We have Status = " + response.getStatus());
            return;
        }
        int FileHandle = response.getHandle();  //File handle provided by the NameNode

        long File_Size = File.length();
        int BytesPerSplit = 64*1024*1024; // 64 MB
        byte[] Buffer = new byte[BytesPerSplit];
        long NumSplits = File_Size / BytesPerSplit;
        long RemainingBytes = File_Size - (NumSplits * BytesPerSplit);
        byte[] LeftBuffer = new byte[(int)RemainingBytes];
        System.out.println(" File Size = " + File_Size + " NumSplits = " + NumSplits + " RemainingBytes " + RemainingBytes);
        System.out.println(" BytesPerSplit = " + BytesPerSplit );


        int tmp;
        for(int i=0; i<=NumSplits; i++)
        {
            try{
                if(i == NumSplits)
                {
                    tmp = bis.read(LeftBuffer);
                }
                else
                {
                    tmp = bis.read(Buffer); 
                }
            }catch(Exception e){
                System.out.println("IOException");
                return;
            }
            //Get a BlockNumber for this Chunk
            AssignBlockRequest.Builder BlockReq = AssignBlockRequest.newBuilder();
            BlockReq.setHandle(FileHandle);
            byte[] Res;
            try{
                Res = this.NNStub.assignBlock(BlockReq.build().toByteArray());
            }catch(Exception e){
                System.out.println("Remote Exception while OpenFile Request!!");
                return;
            }
            AssignBlockResponse BlockRes;
            try{
                BlockRes = AssignBlockResponse.parseFrom(Res);
            }catch(Exception e){
                System.out.println("Remote Procedure Call error while BlockRequest");
                return;
            }
            if(BlockRes.getStatus() < 0)
            {
                System.out.println("Huston, WE have a BlockRes.Status = " + BlockRes.getStatus());
                return;
            }
            BlockLocations newBlock; //To be used to send to the DN as BlockInfo
            newBlock = BlockRes.getNewBlock();
            int BlockNumber = newBlock.getBlockNumber();
            List <DataNodeLocation>  DNLocations = newBlock.getLocationsList();

            //For Sending Write request to the DN
            String SendToIP = DNLocations.get(0).getIp();
            int ToPort = DNLocations.get(0).getPort();
            //Get the DNstub
            //this.DNStub = GetDNStub("Server", SendToIP, ToPort);
            this.DNStub = GetDNStub(DNLocations.get(0).getName(), SendToIP, ToPort);

            //Remove the first DN from the DataNodeLocation list
            int No_DNLocations = newBlock.getLocationsCount();
            BlockLocations.Builder SendBlock = BlockLocations.newBuilder();
            SendBlock.setBlockNumber(BlockNumber);
            for(int a=1; a<No_DNLocations; a++)
            {
                SendBlock.addLocations(DNLocations.get(a));
            }

            WriteBlockRequest.Builder BlockWrite = WriteBlockRequest.newBuilder();
            //Removed the one to which it is being sent
            BlockWrite.setBlockInfo(SendBlock); //Not Same as the one we got from the NN 
            //Set the ByteString
            if(i == NumSplits) //Use LeftBuffer because the file has some bytes left
            {
                BlockWrite.addData(ByteString.copyFrom(LeftBuffer));
            }
            else
            {
                BlockWrite.addData(ByteString.copyFrom(Buffer));
            }

            //Send the WriteBLock to the Data Node
            byte[] Resp;
            try{
                Resp = this.DNStub.writeBlock(BlockWrite.build().toByteArray());
            }catch(Exception e){
                System.out.println("Remote Error while writing to the DN!");
                return;
            }
            WriteBlockResponse Wbr;
            try{
                Wbr = WriteBlockResponse.parseFrom(Resp);
            }catch(Exception e){
                System.out.println("Unknown Error while reading Proto");
                return;
            }
            if(Wbr.getStatus() < 0)
            {
                System.out.println("We have a bad WriteBlockResponse Status = " + Wbr.getStatus());
                return; //Will be problem if half of the file is already written Maybe
            }
        }

        //Close File and get Status
        CloseFileRequest.Builder CloseFile = CloseFileRequest.newBuilder();
        CloseFile.setHandle(FileHandle);
        byte[] FinResp;
        try{
            FinResp = this.NNStub.closeFile(CloseFile.build().toByteArray());
        }catch(Exception e){
            System.out.println("Unable to call CloseFile from put");
            return; 
        }
        CloseFileResponse Resp;
        try{
            Resp = CloseFileResponse.parseFrom(FinResp);
        }catch(Exception e){
            System.out.println("Unable to get proto from CloseFileResponse");
            return;
        }
        if(Resp.getStatus() < 0)
            System.out.println("We have a bad CloseFileResponse Status = " + Resp.getStatus());
        else
            System.out.println("Put to HDFS Successful");
    }

    public void GetFile(String FileName)
    {
        OpenFileRequest.Builder ReadReq = OpenFileRequest.newBuilder();
        ReadReq.setFileName(FileName);
        ReadReq.setForRead(true);  //For Reading
        byte[] Response;
        try{
            Response = this.NNStub.openFile(ReadReq.build().toByteArray());
        }catch(Exception e){
            System.out.println("Remote Exception while OpenFile Request in Getfile!!");
            return;
        }
        OpenFileResponse response; 
        try{
            response = OpenFileResponse.parseFrom(Response);
        }catch(Exception e){
            System.out.println("Proto Problems in ReadRequest Getfile");
            return;
        }
        if(response.getStatus() < 0)
        {
            System.out.println("Huston, We have bad Status = " + response.getStatus());
            return;
        }
        int FileHandle = response.getHandle();

        //Getting Locations of all the blocks
        BlockLocationRequest.Builder BlockLocation = BlockLocationRequest.newBuilder();
        for(int Blocknums : response.getBlockNumsList())
        {
            //For each block, ask for DN details for NN
            BlockLocation.addBlockNums(Blocknums);
        }
        byte[] BlockLocationResp;
        try{
            BlockLocationResp = this.NNStub.getBlockLocations(BlockLocation.build().toByteArray());
        }catch(Exception e){
            System.out.println("Remote Exception while OpenFile Request in Getfile!!");
            return;
        }
        BlockLocationResponse BlockLocationRes; 
        try{
            BlockLocationRes = BlockLocationResponse.parseFrom(BlockLocationResp);
        }catch(Exception e){
            System.out.println("Proto Problems in ReadRequest Getfile");
            return;
        }
        if(BlockLocationRes.getStatus() < 0)
        {
            System.out.println("We have got a bad response for BlockLocation: Status = " + BlockLocationRes.getStatus());
            return;
        }

        // For each Block, ask DN for the block
        ByteString FileBytes = ByteString.EMPTY; //Will Contain all the bytes of the file
        for(BlockLocations Loc : BlockLocationRes.getBlockLocationsList())
        {
            int BlockNumber = Loc.getBlockNumber();
            DataNodeLocation Location = Loc.getLocations(0);  //Getting zeroth Data Nodes
            this.DNStub = GetDNStub(Location.getName(), Location.getIp(), Location.getPort());
            //Request DN for the block and stitch it to the file

            ReadBlockRequest.Builder BlockReq = ReadBlockRequest.newBuilder();
            BlockReq.setBlockNumber(BlockNumber);
            byte[] BlockRes;
            try{
                BlockRes = this.DNStub.readBlock(BlockReq.build().toByteArray());
            }catch(Exception e){
                System.out.println("Remote Exception while ReadFileRequest in Getfile!!");
                return;
            }
            ReadBlockResponse BlockResp; 
            try{
                BlockResp = ReadBlockResponse.parseFrom(BlockRes);
            }catch(Exception e){
                System.out.println("Proto Problems in ReadFileRequest Getfile");
                return;
            }
            if(BlockResp.getStatus() < 0)
            {
                System.out.println("Huston, We have bad Status = " + BlockResp.getStatus());
                return;
            }
            //FileBytes = FileBytes.concat(BlockResp.getData(0));
            for(ByteString A : BlockResp.getDataList())
            {
                FileBytes = FileBytes.concat(A);
            }
        }

        //Copy the FileBytes(ByteString) to a byte array
        byte[] File = new byte[FileBytes.size()];
        FileBytes.copyTo(File, 0);

        //Write File to the Disk
        try{
            FileOutputStream fos = new FileOutputStream(FileName);
            fos.write(File);
            fos.close();
        }catch(Exception e){
            System.out.println("IOError while writing to the file");
            return;
        }
        //System.out.println("File Retrieve Successful");

        //Close File and get Status
        CloseFileRequest.Builder CloseFile = CloseFileRequest.newBuilder();
        CloseFile.setHandle(FileHandle);
        byte[] FinResp;
        try{
            FinResp = this.NNStub.closeFile(CloseFile.build().toByteArray());
        }catch(Exception e){
            System.out.println("Unable to call CloseFile from Get");
            return; 
        }
        CloseFileResponse Resp;
        try{
            Resp = CloseFileResponse.parseFrom(FinResp);
        }catch(Exception e){
            System.out.println("Unable to get proto from CloseFileResponse Get");
            return;
        }
        if(Resp.getStatus() < 0)
            System.out.println("We have a bad CloseFileResponse Status = " + Resp.getStatus());
        else
            System.out.println("File Retrieve Successful");
    }

    public void List() // Done
    {
        ListFilesRequest.Builder ListFiles = ListFilesRequest.newBuilder();
        ListFiles.setDirName(".");

        byte[] Res;
        try{
            Res = this.NNStub.list(ListFiles.build().toByteArray());
        }catch(Exception e){
            System.out.println("Unable to call List from list");
            return; 
        }
        ListFilesResponse Resp;
        try{
            Resp = ListFilesResponse.parseFrom(Res);
        }catch(Exception e){
            System.out.println("Unable to get proto from ListFilesResponse");
            return;
        }
        if(Resp.getStatus() < 0)
        {
            System.out.println("We got a bad response from NN in List: Status = " + Resp.getStatus());
            return;
        }
        //Get the list of files
        System.out.println("The Following are the files in the hdfs");
        for(String FileName : Resp.getFileNamesList())
        {
            System.out.println(FileName); //Print Out Each Filename
        }
    }

    public static void main(String[] args) throws RemoteException, UnknownHostException
    {
        // To read config file and Connect to NameNode
        //Intitalize the Client
        Client Me = new Client();
        System.out.println("This program is written by Shaleen Garg(201401069) & Vinay Khandelwal(201401139)"); 
        System.out.println("Acquiring NameNode stub");

        //Get the Name Node Stub
        //client_config of format Server;IP;Port

           String Config = Me.FileTail("client_config.txt");
           String[] Split_Config = Config.split(";");
           Me.NNStub = Me.GetNNStub(Split_Config[0], Split_Config[1], Integer.parseInt(Split_Config[2]));

        System.out.println("Welcome to SM-HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put Filename
            {
                //Put file into HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
                Me.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}
