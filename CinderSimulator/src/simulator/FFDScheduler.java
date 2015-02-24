package simulator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

public class FFDScheduler extends Scheduler {
	
	//private static final int MAX_Q_LENGTH = 100;
	private static final int MAX_WAIT = 20;
	
	PriorityBlockingQueue<VolumeRequest> requestQueue;
	ArrayList<Backend> candidateList;
	private int numOfAddQFail;
	private int numOfScheduleFail;
	
	public FFDScheduler() {
		super(ScheduleMethod.FFD);
		requestQueue = new PriorityBlockingQueue<VolumeRequest>(MAX_Q_LENGTH);
		candidateList = new ArrayList<Backend>();
		numOfAddQFail = 0;
		numOfScheduleFail = 0;
	}
	
	
	@Override
	public void addNewRequestToQ(VolumeRequest vr) {
		if (requestQueue.size() >= MAX_Q_LENGTH) {
			numOfAddQFail ++;
			logger.writeToEventLog("INSERT Q FAIL: " + numOfAddQFail + " VR: " + vr.getId());
			//failCap += vr.getSize();
			logger.writeToFailCapLog(Integer.toString(vr.getSize() /1000));
			return;
		} else {
			requestQueue.add(vr);
			//logger.writeToEventLog("INSERT Q VR: " + vr.toString() + " " + this.qToString());
		}
	}
		
	// algorithm 1
	@Override
	public void schedule(ArrayList<Backend> backendList) {
		// weight calculation is done at constructor of VolumeRequest
		// request queue sorting is done by PriorityQueue
		
		Backend candidateBackend = null;
		
		Iterator<VolumeRequest> vrIt = requestQueue.iterator();
		while (vrIt.hasNext()) {
			VolumeRequest vr = (VolumeRequest) vrIt.next();
			candidateBackend = null;
/*			for (Backend backend : backendList) {

				// check 1st dimension: capacity
				if (backend.getAllocatedCapacity() + vr.getSize() < Backend.MAX_CAPACITY) {
					candidateBackend = backend;
					// check 2nd dimension: IOPS
					if (VolumeRequest.DIMENTION == 2) {
						if (backend.getAvailableVolumeSpeed() >= vr.getSLA()) {
							// if pass all dimension check
							candidateBackend = backend;
							
							break;
						} else {
							candidateBackend = null;
						}
					}

				}
			}*/
			
			filtering(backendList, vr);
			if (candidateList.isEmpty()) {
				continue;
			} else {
				Collections.sort(candidateList);
				candidateBackend = candidateList.get(0);
			}
			// if there is a backend can serve the request
						// then create volume on that backend
			if (candidateBackend != null) {
				Volume volume = candidateBackend.createVolume(vr);
				candidateBackend.readVolume(volume);
				//vr.served = true;
				vrIt.remove();
				candidateBackend.calWeight();
				
				logger.writeToEventLog("SCHEDULE SUCCESS VR: " + vr.toString() + " to "	+ candidateBackend.getId());
				//System.out.println("candidate selected length: " + requestQueue.size());
				//System.out.println("serve request " + vr.toString() + " on backend " + candidateBackend.getId());
			} else {
				numOfScheduleFail ++;
				logger.writeToEventLog("SCHEDULE FAIL " + numOfScheduleFail + " VR: " + vr.getId());
			}
		}
		
		/*for (VolumeRequest vr : requestQueue) {
			candidateBackend = null;
			for (Backend backend : backendList) {

				// check 1st dimension: capacity
				if (backend.getAllocatedCapacity() + vr.getSize() < Backend.MAX_CAPACITY) {
					candidateBackend = backend;
					// check 2nd dimension: IOPS
					if (VolumeRequest.DIMENTION == 2) {
						if (backend.getAvailableVolumeSpeed() >= vr.getSLA()) {
							// if pass all dimension check
							candidateBackend = backend;
							
							break;
						} else {
							candidateBackend = null;
						}
					}

				}
			}
			// if there is a backend can serve the request
			// then create volume on that backend
			if (candidateBackend != null) {
				Volume volume = candidateBackend.createVolume(vr);
				candidateBackend.readVolume(volume);
				//vr.served = true;
				requestQueue.remove(vr);
				
				logger.writeToEventLog("SCHEDULE SUCCESS VR: " + vr.toString() + " to "	+ candidateBackend.toString());
				//System.out.println("candidate selected length: " + requestQueue.size());
				//System.out.println("serve request " + vr.toString() + " on backend " + candidateBackend.getId());
			} else {
				numOfScheduleFail ++;
				logger.writeToEventLog("SCHEDULE FAIL " + numOfScheduleFail + " VR: " + vr.getId());
			}
		}*/
		
		//checkExpireRequests();
		
		//System.out.println("schedule done queue length: " + requestQueue.size() + " at " + Simulator.timer);
	}
	
	// 2-dimention filtering
	private void filtering(ArrayList<Backend> backendList, VolumeRequest vr) {
		
		candidateList.clear();
		//String bId = " ";
		for (Backend backend : backendList) {
			// check dimension 1: capacity
			if ( backend.getAllocatedCapacity() + vr.getSize() <= Backend.MAX_CAPACITY ) {
				if (VolumeRequest.DIMENTION == 2) {
					// check dimension 2: IOPS
					if(backend.getAvailableVolumeSpeed() >= vr.getSLA() ) {
						candidateList.add(backend);
					}
				} else {
					candidateList.add(backend);
				}
				//bId += Integer.toString(backend.getId()) + " ";
			}
		}
		
		//logger.writeToEventLog("FILTER DONE list: " + bId);
	}
	
	private void filteringTh(ArrayList<Backend> backendList, VolumeRequest vr) {
		candidateList.clear();
		//String bId = " ";
		for (Backend backend : backendList) {
			// check dimension 1: capacity
			if ( backend.getAllocatedCapacity() + vr.getSize() <= Backend.CAP_UP_TH && backend.getAllocatedCapacity() + vr.getSize() >= Backend.CAP_DOWN_TH ) {
				if (VolumeRequest.DIMENTION == 2) {
					// check dimension 2: IOPS
					if(backend.getAvailableVolumeSpeed() >= vr.getSLA() && backend.getAvailableVolumeSpeed() >= Backend.IOPS_DOWN_TH && backend.getAvailableVolumeSpeed() <= Backend.IOPS_UP_TH ) {
						candidateList.add(backend);
					}
				} else {
					candidateList.add(backend);
				}
				//bId += Integer.toString(backend.getId()) + " ";
			}
		}
	}
	
	private void checkExpireRequests() {
		Iterator<VolumeRequest> it = requestQueue.iterator();
		while (it.hasNext()) {
			VolumeRequest vr = (VolumeRequest) it.next();
			if (vr.getWait() >= MAX_WAIT) {
				logger.writeToEventLog("REQUEST EXPIRE VR: " + vr.toString());
				it.remove();
			} else {
				vr.increaseWait();
			}
		}
		
/*		for (VolumeRequest vr: requestQueue) {
			if (vr.getWait() >= MAX_WAIT) {
				logger.writeToEventLog("REQUEST EXPIRE VR: " + vr.toString());
				requestQueue.remove(vr);
			} else {
				vr.increaseWait();
			}
		}*/
	}
	
	private String qToString() {
		String str = "	";
		for (VolumeRequest vr : requestQueue) {
			str += vr.getId() + " ";
		}
		return str;
	}

	@Override
	public boolean isQEmpty() {
		return requestQueue.isEmpty();
	}

}
