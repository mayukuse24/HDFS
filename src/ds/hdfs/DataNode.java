//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.io.*;
import ds.hdfs.hdfsformat.*;
import ds.hdfs.IDataNode.*;

public class DataNode implements IDataNode
{
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;

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
    }

    public byte[] writeBlock(byte[] Inp)
    {
    }

    public void BlockReport()
    {
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

    public static void main(String args[])
    {
        //Define a Datanode Me
        DataNode Me = new DataNode();        
        
        //Bind The DataNode Self
        String Config = Me.FileTail("dn_config.txt");
        String[] Split_Config = Config.split(";");
        Me.BindServer(Split_Config[0], Split_Config[1], Integer.parseInt(Split_Config[2]));
        Me.MyIP = Split_Config[1];
        Me.MyPort = Integer.parseInt(Split_Config[2]);        

        //Get The NameNode
        String NNConfig = Me.FileTail("nn_details.txt");
        String[] NNSplit_Config = NNConfig.split(";");
        Me.NNStub = Me.GetNNStub(NNSplit_Config[0], NNSplit_Config[1], Integer.parseInt(NNSplit_Config[2]));

        
    }
}
