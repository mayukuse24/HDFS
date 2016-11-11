package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.*;

import ds.hdfs.hdfsformat.*;

public class NameNode implements INameNode{

	String ip,name;
	int port;
	protected Registry serverRegistry;
	Map<Integer, List<Integer>> chunktodns = new HashMap<Integer, List<Integer>>();
	List<FileInfo> filelist = new ArrayList<FileInfo>(); 
	DataNode[] dninfo; //index stands for datanode id
	List<Integer> DNlist = new ArrayList<Integer>();
	String fchunk_file = new String();
	
	public NameNode(String addr,int p, String nn)
	{
		ip = addr;
		port = p;
		name = nn;
	}
	
	public static class DataNode
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
	
	public static class FileInfo
	{
		String filename;
		int filehandle;
		boolean writemode;
		ArrayList<Integer> Chunks;
		public FileInfo(String name, int handle, boolean option)
		{
			filename = name;
			filehandle = handle;
			writemode = option;
			Chunks = new ArrayList<Integer>();
		}
	}
	/* Method to open a file given file name with read-write flag*/
	
	boolean findInFilelist(int fhandle)
	{
		for(int i=0;i<filelist.size();i++)
		{
			if(filelist.get(i).filehandle == fhandle)
				return true;
		}
		return false;
	}
	
	public void printFilelist()
	{
		for(int i=0;i<filelist.size();i++)
		{
			System.out.println(filelist.get(i).filehandle + "  " + filelist.get(i).filename);
		}
	}
	
	public byte[] openFile(byte[] inp) throws RemoteException
	{
		OpenFileResponse.Builder response = OpenFileResponse.newBuilder();
		try
		{
			OpenFileRequest deserObj = OpenFileRequest.parseFrom(inp);
			String filename = deserObj.getFileName();
			boolean toRead = deserObj.getForRead();
					
			response.setStatus(-1); //set fail as default
			
			int random = (int)((Math.random() * 10000) + 1); //get random chunk number
			while(findInFilelist(random)) //chunk is unique
			{
				random++;
			}
			int filehandle = random; //assign a random unique number	
			
			System.out.println("filehandle for " + filename + " = " + Integer.toString(filehandle));
			response.setHandle(filehandle);
			
			if(toRead) //read request
			{
				filelist.add(new FileInfo(filename,filehandle,false));
				
				File ftest = new File(fchunk_file);
				ftest.createNewFile(); //creates a new file only if one doesnt exist
				
				FileInputStream fis = new FileInputStream(fchunk_file);
				
				//Construct BufferedReader from InputStreamReader
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			 
				//line format : filename:chunk1,chunk2,chunk3
				String line = null;
				while ((line = br.readLine()) != null) 
				{
					System.out.println(line);
					String[] fname_chunks = line.split(":");
					if(filename.equals(fname_chunks[0]))
					{
						fname_chunks = fname_chunks[1].split(",");
						for(int i=0;i<fname_chunks.length;i++)
						{
							response.addBlockNums(Integer.parseInt(fname_chunks[i]));
						}
						response.setStatus(1);
						break;
					}
				}
				br.close();
			}
			else //write request
			{
				filelist.add(new FileInfo(filename,filehandle,true));
				/*
				File ftest = new File(fchunk_file);
				ftest.createNewFile(); //creates a new file only if one doesnt exist
				
				//Open filechunkslist.txt and write in filename only
				BufferedWriter out = new BufferedWriter(new FileWriter(fchunk_file, true));
				out.append(filename+"\n");
				out.close();*/
				response.setStatus(1);
			}
		}
		catch (Exception e) 
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	public byte[] closeFile(byte[] inp ) throws RemoteException
	{
		CloseFileResponse.Builder response = CloseFileResponse.newBuilder();
		try
		{
			CloseFileRequest deserObj = CloseFileRequest.parseFrom(inp);
			int filehandle = deserObj.getHandle();
			for(int i=0;i<filelist.size();i++)
			{
				if(filehandle == filelist.get(i).filehandle)
				{
					//If write mode then append line to filetochunklist.txt
					if(filelist.get(i).writemode)
					{
						String fname = filelist.get(i).filename;
						
						File ftest = new File(fchunk_file);
						ftest.createNewFile(); //creates a new file only if one doesnt exist
					
						BufferedWriter bw = new BufferedWriter(new FileWriter(fchunk_file, true));
						ArrayList<Integer> tempfiletochunklist = filelist.get(i).Chunks;
						String lastline = fname + ":"+ tempfiletochunklist.get(0);
						for(int k=1;k<tempfiletochunklist.size();k++)
						{
							lastline += ","+tempfiletochunklist.get(k);
						}
						bw.append(lastline + "\n");
						bw.close();
						
					}
					//Remove handle from temporary filelist
					filelist.remove(i);
					break;
				}
			}
			response.setStatus(1);
		}
		catch(Exception e)
		{
			System.err.println("Error at closefileRequest " + e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
	
	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		BlockLocationResponse.Builder response = BlockLocationResponse.newBuilder();
		try
		{
			BlockLocationRequest deserObj = BlockLocationRequest.parseFrom(inp);
			this.printMsg("Read block request from client");
			for(int i=0;i<deserObj.getBlockNumsCount();i++)
			{
				int blockno = deserObj.getBlockNums(i);
				BlockLocations.Builder blockinfo = BlockLocations.newBuilder();
				blockinfo.setBlockNumber(blockno);
				while(true)
				{
					List<Integer> dnlist = chunktodns.get(blockno); //TODO : Test for empty dnlist
				
					this.printMsg("list of DN " + dnlist + " of chunk number " + blockno);
					
					if(dnlist == null) //chunk doesnt exist
					{
						System.out.println("Chunk " + Integer.toString(blockno) + " does not exist");
						response.setStatus(-1);
						return response.build().toByteArray();
						
					}else if(dnlist.size() == 0) //DN hasnt contacted NN with chunks yet. But chunk exists
					{
						System.out.println("Chunk " + Integer.toString(blockno) + " has no DNs yet. Retrying...");
						TimeUnit.SECONDS.sleep(3);
					}else
					{
						Collections.shuffle(dnlist);
						for(int j=0;j<dnlist.size();j++)
						{
							int dnid = dnlist.get(j);
							DataNodeLocation dnobj = DataNodeLocation.newBuilder()
									.setIp(this.dninfo[dnid].ip)
									.setPort(this.dninfo[dnid].port)
									.setName(this.dninfo[dnid].serverName).build();
							blockinfo.addLocations(dnobj);
						}
						break;
					}
				}
				response.addBlockLocations(blockinfo);
			}
			
			this.printMsg("Responding to readblock request");
			response.setStatus(1);
		}
		catch(Exception e)
		{
			System.err.println("Error at getBlockLocations "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}		
		return response.build().toByteArray();
	}
	
	
	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		AssignBlockResponse.Builder response = AssignBlockResponse.newBuilder();
		try
		{
			AssignBlockRequest deserObj = AssignBlockRequest.parseFrom(inp);
			this.printMsg("Write Block Request from client");
			String fname = new String();
			int fileindex = -1;
			int filehandle = -1;
			for(int i=0;i<filelist.size();i++)
			{
				if(deserObj.getHandle() == filelist.get(i).filehandle)
				{
					fileindex = i;
					fname = filelist.get(i).filename;
					filehandle = filelist.get(i).filehandle;
				}
			}
			
			if(fname == null) //handle filehandle that doesnt exist in NN
			{
				response.setStatus(-2); //file not present error
				return response.build().toByteArray();
			}

			
			int random = (int)(Math.random() * 10000 + 1); //get random chunk number 
			while(chunktodns.get(random) != null && random < dninfo.length) //chunk is unique
			{
				random++;
			}
			
			chunktodns.put(random, new ArrayList<Integer>());
			
			BlockLocations.Builder blockobj = BlockLocations.newBuilder().setBlockNumber(random);
			
			this.printMsg("Creating Assigning chunk number " + Integer.toString(random) + " filename " + fname + " for filehandle " + Integer.toString(filehandle) + " indexed at " + Integer.toString(fileindex));
			
			//Record file to chunk relation now. Write to filetochunklist.txt when closing file
			filelist.get(fileindex).Chunks.add(random);
			
			Collections.shuffle(DNlist);
			for(int i=0;i<3;i++) //Select random 2 Datanodes
			{
				DataNodeLocation dnobj = DataNodeLocation.newBuilder().setIp(dninfo[DNlist.get(i)].ip).setPort(dninfo[DNlist.get(i)].port).setName(dninfo[DNlist.get(i)].serverName).build();
				blockobj.addLocations(dnobj);
			}
			
			blockobj.setBlockNumber(random);
			response.setNewBlock(blockobj);
			
			this.printMsg("Responding to writeblock request for chunk" + Integer.toString(random));
		}
		catch(Exception e)
		{
			System.err.println("Error at AssignBlock "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		
		return response.build().toByteArray();
	}
		
	
	public byte[] list(byte[] inp ) throws RemoteException
	{
		ListFilesResponse.Builder response = ListFilesResponse.newBuilder();
		
		try
		{
			File ftest = new File(fchunk_file);
			ftest.createNewFile(); //creates a new file only if one doesnt exist
			
			FileInputStream fis = new FileInputStream(fchunk_file);
			
			//Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		 
			//line format : filename:chunk1,chunk2,chunk3
			String line = null;
			while ((line = br.readLine()) != null) 
			{
				String[] fname_chunks = line.split(":");
				response.addFileNames(fname_chunks[0]);
			}
			
			response.setStatus(1);
			br.close();
		}catch(Exception e)
		{
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	// Datanode <-> Namenode interaction methods
		
	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		BlockReportResponse.Builder response = BlockReportResponse.newBuilder();
		try
		{
			BlockReportRequest deserObj = BlockReportRequest.parseFrom(inp);
			
			int dnid = deserObj.getId();
			this.printMsg("BlockRequest From DN" +Integer.toString(dnid));
			if(dninfo[dnid] == null)
			{
				DNlist.add(dnid);
				dninfo[dnid] = new DataNode(deserObj.getLocation().getIp(),deserObj.getLocation().getPort(),deserObj.getLocation().getName());
			}
			
			for(int i=0;i<deserObj.getBlockNumbersCount();i++)
			{
				int blockno = deserObj.getBlockNumbers(i);
				List<Integer> dnlist = chunktodns.get(blockno); 
				response.addStatus(-2); //default status is failed
				
				if(dnlist == null)
				{	
					dnlist = new ArrayList<Integer>();
					dnlist.add(dnid);
				}
				else if(!dnlist.contains(dnid))
				{
					dnlist.add(dnid);
				}
				
				chunktodns.put(blockno, dnlist);
				response.setStatus(i, 1); //on succesful find
			}
		}
		catch(Exception e)
		{
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
			response.addStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	
	
	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		HeartBeatResponse response = HeartBeatResponse.newBuilder().setStatus(1).build();
		return response.toByteArray();
	}
	
	public void printMsg(String msg)
	{
		System.out.println(msg);		
	}
	
	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
		FileInputStream fis = new FileInputStream("nn_details.txt");
		
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		NameNode NN = null;
		
		String line = null;
		while ((line = br.readLine()) != null) 
		{
			System.out.println(line);
			String[] Split_Config = line.split(";");
			if("NN".equals(Split_Config[0]))
			{
				NN = new NameNode(Split_Config[1],Integer.parseInt(Split_Config[2]),Split_Config[0]);
				break;
			}
		}
		br.close();
		
		if(NN == null)
		{
			System.out.println("Failed to configure NN. Check nn_details.txt file");
			System.exit(-1);
		}
		
		NN.dninfo = new DataNode[15000];
		NN.fchunk_file = "filetochunklist.txt";
		
		System.setProperty("java.rmi.server.hostname", NN.ip);
		System.setProperty("java.security.policy","test.policy");

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        		
		//final NetworkTest serverObj = new NetworkTest(args[0]);
		try {
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(NN, 0);

			// Bind the remote object's stub in the registry
			NN.serverRegistry = LocateRegistry.getRegistry(NN.port);
			NN.serverRegistry.rebind(NN.name, stub);

			System.err.println(NN.name + " ready");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString() + " Failed to start server");
			e.printStackTrace();
		}
	}
	
	//TEST CODE
	/*
	public static void main(String[] args) throws RemoteException, InvalidProtocolBufferException
	{
		NameNode NN = new NameNode();
		NN.dninfo = new DataNode[15000];
		NN.fchunk_file = "filetochunklist.txt";
		/*
		try
		{
			File ftest = new File("configfile.txt");
			ftest.createNewFile(); //creates a new file only if one doesnt exist
			
			FileInputStream fis = new FileInputStream("configfile.txt");
			
			//Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		 
			//line format : filename:chunk1,chunk2,chunk3
			String line = null;
			while ((line = br.readLine()) != null) 
			{
				String[] fname_chunks = line.split(",");
				if("DN".equals(fname_chunks[0]))
				{
					NN.dninfo[Integer.parseInt(fname_chunks[1])] = new DataNode(fname_chunks[2],Integer.parseInt(fname_chunks[3]));
					NN.DNlist.add(Integer);
				}
			}
			br.close();
		}
		catch(Exception e)
		{
			System.err.println("Error importing configfile at main");
			e.printStackTrace();
		} 
		
		
		OpenFileRequest req = OpenFileRequest.newBuilder().setFileName("testfile.txt").setForRead(true).build();
		
		byte[] res = NN.openFile(req.toByteArray());
		OpenFileResponse response = OpenFileResponse.parseFrom(res);
		
		NN.printMsg(Integer.toString(response.getHandle()) + " " + Integer.toString(response.getStatus()));
		NN.printMsg(response.toString());
		
		NN.chunktodns.put(1982, new ArrayList<Integer>(Arrays.asList(1,3,4)));
		NN.chunktodns.put(1234, new ArrayList<Integer>(Arrays.asList(2,3,5)));
		
		BlockLocationRequest req2 = BlockLocationRequest.newBuilder().addBlockNums(1982).addBlockNums(1234).build();
		res = NN.getBlockLocations(req2.toByteArray());
		
		BlockLocationResponse response2 = BlockLocationResponse.parseFrom(res);
		NN.printMsg(response2.toString()); */
		
		/*
		NN.chunktodns.put(1982, new ArrayList<Integer>(Arrays.asList(1,3,4)));
		NN.chunktodns.put(231, new ArrayList<Integer>(Arrays.asList(2,1,5)));
		NN.chunktodns.put(451, new ArrayList<Integer>(Arrays.asList(1,4,5)));
		NN.DNlist.add(1);
		NN.DNlist.add(2);
		NN.DNlist.add(3);
		NN.DNlist.add(4);
		NN.DNlist.add(5);*/
		
		/*
		BlockReportRequest request12 = BlockReportRequest.newBuilder().setId(4).addBlockNumbers(23).addBlockNumbers(2312).build();
		byte[] temp = NN.blockReport(request12.toByteArray());
		BlockReportResponse response12 = BlockReportResponse.parseFrom(temp);
		NN.printMsg(response12.toString());*/
		
		
		/*
		OpenFileRequest req = OpenFileRequest.newBuilder().setFileName("testfile.txt").setForRead(false).build();
		OpenFileRequest req2 = OpenFileRequest.newBuilder().setFileName("testfile2.txt").setForRead(false).build();
		OpenFileRequest req3 = OpenFileRequest.newBuilder().setFileName("testfile3.txt").setForRead(false).build();
		OpenFileRequest req4 = OpenFileRequest.newBuilder().setFileName("testfile4.txt").setForRead(false).build();
		
		byte[] res = NN.openFile(req.toByteArray());
		byte[] res2 = NN.openFile(req2.toByteArray());
		byte[] res3 = NN.openFile(req3.toByteArray());
		byte[] res4 = NN.openFile(req4.toByteArray());
		
		
		BlockReportRequest req21 = BlockReportRequest.newBuilder().setId(1)
				.setLocation(DataNodeLocation.newBuilder().setIp("1.2.3.4").setPort(1321).build())
				.addBlockNumbers(1982).addBlockNumbers(451).addBlockNumbers(231).build();
		
		BlockReportRequest req22 = BlockReportRequest.newBuilder().setId(2)
				.setLocation(DataNodeLocation.newBuilder().setIp("2.5.6.7").setPort(2441).build())
				.addBlockNumbers(231).build();
		
		BlockReportRequest req23 = BlockReportRequest.newBuilder().setId(3)
				.setLocation(DataNodeLocation.newBuilder().setIp("3.2.3.4").setPort(3454).build())
				.addBlockNumbers(1982).build();
		
		BlockReportRequest req24 = BlockReportRequest.newBuilder().setId(4)
				.setLocation(DataNodeLocation.newBuilder().setIp("4.1.1.4").setPort(4451).build())
				.addBlockNumbers(1982).addBlockNumbers(451).build();
		
		BlockReportRequest req25 = BlockReportRequest.newBuilder().setId(5)
				.setLocation(DataNodeLocation.newBuilder().setIp("5.5.3.4").setPort(5111).build())
				.addBlockNumbers(231).addBlockNumbers(451).build();
		
		BlockReportResponse res21 = BlockReportResponse.parseFrom(NN.blockReport(req21.toByteArray()));
		BlockReportResponse res22 = BlockReportResponse.parseFrom(NN.blockReport(req22.toByteArray()));
		BlockReportResponse res23 = BlockReportResponse.parseFrom(NN.blockReport(req23.toByteArray()));
		BlockReportResponse res24 = BlockReportResponse.parseFrom(NN.blockReport(req24.toByteArray()));
		BlockReportResponse res25 = BlockReportResponse.parseFrom(NN.blockReport(req25.toByteArray()));
		
		NN.printMsg(res21.toString());
		NN.printMsg(res22.toString());
		NN.printMsg(res23.toString());
		NN.printMsg(res24.toString());
		NN.printMsg(res25.toString());
		
		NN.printMsg(NN.DNlist.toString());
		NN.printMsg(NN.chunktodns.toString());
		NN.printFilelist();
		
		AssignBlockRequest req11 = AssignBlockRequest.newBuilder().setHandle(NN.filelist.get(2).filehandle).build();
		AssignBlockResponse res11 = AssignBlockResponse.parseFrom(NN.assignBlock(req11.toByteArray()));
		AssignBlockRequest req12 = AssignBlockRequest.newBuilder().setHandle(NN.filelist.get(2).filehandle).build();
		AssignBlockResponse res12 = AssignBlockResponse.parseFrom(NN.assignBlock(req12.toByteArray()));
		AssignBlockRequest req13 = AssignBlockRequest.newBuilder().setHandle(NN.filelist.get(2).filehandle).build();
		AssignBlockResponse res13 = AssignBlockResponse.parseFrom(NN.assignBlock(req13.toByteArray()));
		
		NN.printMsg(res11.toString());
		NN.printMsg(res12.toString());
		NN.printMsg(res13.toString());
		*/
	//}
	
}
