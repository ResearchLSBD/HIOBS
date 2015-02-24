package simulator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;


public class DefaultScheduler extends Scheduler {

	LinkedList<VolumeRequest> requestQueue;
	static ArrayList<Backend> candidateList;
	
	public int numOfNoCandidate;
	
	public DefaultScheduler() {
		super(ScheduleMethod.DEFAULT);
		requestQueue = new LinkedList<VolumeRequest>() ;
		candidateList = new ArrayList<Backend>();
		
		numOfNoCandidate = 0;
	}


	@Override
	public void schedule(ArrayList<Backend> backendList) {
		
		Iterator<VolumeRequest> itVr = requestQueue.iterator();
		while (itVr.hasNext()) {
			VolumeRequest vr = (VolumeRequest) itVr.next();
			filtering(backendList, vr.getSize());
			
			if (candidateList.size() == 0) {
				numOfNoCandidate++;
				//logger.writeToEventLog("SCHEDULE FAIL: NO CANDIDATE !!! 	" + numOfNoCandidate + "	" + vr.toString());
			} else {
				Backend backend = weighting();
				logger.writeToEventLog("SCHEDULE SUCCESS VR: " + vr.toString() + " to " + backend.getId());
				Volume v = backend.createVolume(vr);
				backend.readVolume(v);
				itVr.remove();
				return;
			}
		}
		
	}
	
	public static Backend migrationSchedule(MigrationTask mTask) {
		
		ArrayList<Backend> backendList = new ArrayList<Backend>(Simulator.getBackendList());
		backendList.remove(mTask.out);
		Backend candidate = null;
		
		candidateList.clear();
		for (Backend backend : backendList) {
			if (backend.getAllocatedCapacity() + mTask.volume.getSize() <= Backend.CAP_UP_TH) {
				candidateList.add(backend);
			}
		}
		
		if (candidateList.isEmpty()) {
			return null;
		} else {
			return weighting();
		}		
	}
	
	private void filtering(ArrayList<Backend> backendList, int size) {
		
		candidateList.clear();
		String bId = " ";
		for (Backend backend : backendList) {
			if (backend.getAllocatedCapacity() + size <= Backend.MAX_CAPACITY) {
				candidateList.add(backend);
				bId += Integer.toString(backend.getId()) + " ";
			}
		}
		
		//logger.writeToEventLog("FILTER DONE list: " + bId);
	}
	
	private static Backend weighting() {
		Collections.sort(candidateList, new Comparator<Backend>() {

			@Override
			public int compare(Backend o1, Backend o2) {
				return (Backend.MAX_CAPACITY - o2.getAllocatedCapacity()) - (Backend.MAX_CAPACITY - o1.getAllocatedCapacity());
			}
		});
		if (Simulator.DEBUG) {
			String bId = "availSpace ";
			for (Backend backend : candidateList) {
				bId += Integer.toString(backend.getId()) + " " + (Backend.MAX_CAPACITY - backend.getAllocatedCapacity() + " ");
			}
			
			//logger.writeToEventLog("WEIGHTING DONE list: " + bId);
		}
		
		return candidateList.get(0);
	}

	@Override
	public void addNewRequestToQ(VolumeRequest vr) {
		if (requestQueue.size() >= MAX_Q_LENGTH) {
			logger.writeToEventLog("INSERT Q FAIL: VR: " + vr.getId() + " Q SIZE:" + requestQueue.size());
			
			logger.writeToFailCapLog(Integer.toString(vr.getSize()/1000));
			//failCap += vr.getSize();
			return;
		} else {
			requestQueue.add(vr);
			logger.writeToEventLog("INSERT Q VR: " + vr.toString() );
		}
		
		//logger.writeToEventLog("INSERT Q VR: " + vr.toString() + " " + this.qToString());
	}

	@Override
	public boolean isQEmpty() {
		return requestQueue.isEmpty();
	}

}
