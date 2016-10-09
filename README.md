# HDFS

docker images
docker run --name vindock -e http_proxy -e https_proxy --net=host --rm -ti ds/arch:latest /bin/bash 
docker exec -i -t vindock /bin/bash
docker commit vindock ds/arch
docker ps
docker ps -a
docker cp Ricard-Agrawala\ Assignment-2 vindock:/home
docker inspect -f '{{.Name}} - {{.NetworkSettings.IPAddress }}' $(docker ps -aq)


NameNode Functions :-

	/* OpenFileResponse openFile(OpenFileRequest) */
	/* Method to open a file given file name with read-write flag*/
	byte[] openFile(byte[] inp) throws RemoteException;
	
	/* CloseFileResponse closeFile(CloseFileRequest) */
	byte[] closeFile(byte[] inp ) throws RemoteException;
	
	/* BlockLocationResponse getBlockLocations(BlockLocationRequest) */
	/* Method to get block locations given an array of block numbers */
	byte[] getBlockLocations(byte[] inp ) throws RemoteException;
	
	/* AssignBlockResponse assignBlock(AssignBlockRequest) */
	/* Method to assign a block which will return the replicated block locations */
	byte[] assignBlock(byte[] inp ) throws RemoteException;
	
	/* ListFilesResponse list(ListFilesRequest) */
	/* List the file names (no directories needed for current implementation */
	byte[] list(byte[] inp ) throws RemoteException;
	
	/*
		Datanode <-> Namenode interaction methods
	*/
	
	byte[] blockReport(byte[] inp ) throws RemoteException;
	
	byte[] heartBeat(byte[] inp ) throws RemoteException;
