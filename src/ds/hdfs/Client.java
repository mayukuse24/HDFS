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

public class Client{
    public static void main(String[] args) throws RemoteException, UnknownHostException{
        // To read config file and Connect to NameNode
        //
        System.out.println("Welcome to SM-HDFS!!");
        System.out.println("This program is written by Shaleen Garg(201401069) & Vinay Khandelwal(201401139)");
        Scanner Scan = new Scanner(System.in);            
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.next();
            String[] Split_Commands = Command.split(" ");
            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS");
                System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))
            {
                //Put file into HDFS
                try{
                    System.out.println("Put" + Split_Commands[1]);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                try{
                System.out.println("Get" + Split_Commands[1]);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}
