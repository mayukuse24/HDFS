package ds.mapreduce;

import java.rmi.RemoteException;

public interface IJobTracker {
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] inp) throws RemoteException;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] inp) throws RemoteException;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] inp) throws RemoteException;
}
