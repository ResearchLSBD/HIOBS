package simulator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import simulator.Volume.VolumeStatus;

public class Backend implements Comparable<Backend>{
	
	
	//public static final int MAX_CAPACITY = 18000000; //MB 18TB 60 SAS 15k HDs
	//public static final int MAX_BANDWIDTH = 8800; // 0.7 read / 0.3 write / 8k block
	
	public static final int MAX_CAPACITY = 2394000; //GB 800*3TB SATA 7200rpm RAID6
	public static final int MAX_BANDWIDTH = 60800; // 0.7 read / 0.3 write / 8k block
	
	// 512k block sequential
	// 260 IOPS single disk
	public static final int MAX_READ_IOPS = 15600; 	// 1.0 read  
	public static final int MAX_WRITE_IOPS = 7800;	// 1.0 write
	public static final double SEQ_BLOCK = 0.5; // MB
	public static final int MAX_READ_MB = (int) (MAX_READ_IOPS * SEQ_BLOCK);
	public static final int MAX_WRITE_MB = (int) (MAX_WRITE_IOPS * SEQ_BLOCK);
	
	public static double UP_TH = 0.9;
	public static double DOWN_TH = 0.1;
	
	public static int CAP_UP_TH = (int) ( UP_TH * MAX_CAPACITY);
	public static int CAP_DOWN_TH = (int) (DOWN_TH * MAX_CAPACITY);
	
	public static int IOPS_UP_TH = (int) (UP_TH * MAX_BANDWIDTH);
	public static int IOPS_DOWN_TH = (int) (DOWN_TH * MAX_BANDWIDTH);
	
	private static int idGen = 0;
	
	private int id;
	private int currentThroughput;
	private int volumeSpeed;
	private int availableVolumeSpeed;
	public int allocatedCapacity;
	public int validAllocatedCapacity;
	public int volumeFullReadIOPS;
	public int volumeFullWriteIOPS;
	public int numOfIO;
	public int numOfMigrating;
	private int SLAvio;
	public int weight;
	public boolean hasMigratingVolume;
	private PriorityQueue<Volume> volumes;
	private ArrayList<MigrationTask> migratingTasks;
	
	private Logger logger = Logger.getLoggerInstance();

	public Backend() {
		id = idGen++;
		this.allocatedCapacity = 0;
		this.currentThroughput = 0;
		this.volumeSpeed = 0;
		this.availableVolumeSpeed = MAX_BANDWIDTH;
		this.volumes = new PriorityQueue<Volume>();
		this.numOfIO = 0;
		this.numOfMigrating = 0;
		this.migratingTasks = new ArrayList<MigrationTask>();
		this.SLAvio = 0;
		this.weight = calWeight();
		hasMigratingVolume = false;
		validAllocatedCapacity = 0;
	}
		

	public boolean readVolume(Volume v) {
		
		if (v.getStatus() != VolumeStatus.IO && v.getStatus() != VolumeStatus.MIGRATING) {
			changeVolumeStatus(v, VolumeStatus.IO);
			
			checkVolumeStatus();
			logger.writeToEventLog("READ VOL " + v.toString() + " " + this.id);
			
			return true;
		}
		return false;
	}
	
	private boolean removeVolume(Volume v) {
		//logger.writeToEventLog("BEFOR DELETE VOLUME " + v.toString() + " at "+ this.toString());
		if (v.getStatus() == VolumeStatus.MIGRATING) {
			Iterator<MigrationTask> mTaskIt = migratingTasks.iterator();
			boolean isOut = false;
			// iterate the migration list to find the task
			while (mTaskIt.hasNext()) {
				MigrationTask mTask = (MigrationTask) mTaskIt.next();
				// if the task is in this backend, which means this backend is the out-backend
				// delete the migration task of this volume
				if (mTask.volume == v) {
					// delete volume in in-backend
					mTask.volumeExpire();
					logger.writeToMigEventLog("DELETE MIGRATING VOLUME " + v.toString() + " at " + toString());
					mTaskIt.remove();
					isOut = true;
					break;
				} 
			}
			// if this backend is the in-backend
		} 
		// delete it in this backend's volume list
		allocatedCapacity -= v.getSize();
		validAllocatedCapacity -= v.getSize();
		if (v.getStatus() == VolumeStatus.IO) {
			numOfIO--;
		} else if (v.getStatus() == VolumeStatus.MIGRATING) {
			numOfMigrating--;
		}
		
		//changeVolumeStatus(v, VolumeStatus.IDLE);
		//volumes.remove(v);
		//checkVolumeStatus();
			
		if (Simulator.DEBUG) {
			System.out.println("DELETE " + v.toString() + " at " + Simulator.getTimer());
		}
		//logger.writeToEventLog("DELETE VOLUME " + v.toString() + " at "+ this.toString());
		return true;
	}
	
	
	public void updateSpeed() {
		if (volumes.isEmpty()) {
			currentThroughput = 0;
			volumeSpeed = 0;
			volumeFullReadIOPS = 0;
			volumeFullWriteIOPS = 0;
			availableVolumeSpeed = MAX_BANDWIDTH;
		} else {
			int outMigNum = migratingTasks.size();
			if (numOfMigrating < outMigNum) {
				String out = "";
				for (MigrationTask mTask : migratingTasks) {
					out += mTask.volume.getId() + " "; 
				}
				logger.writeToMigEventLog("VOLUME COUNT ERROR total mig < outMig " + out + " at " + toString());
			}
			currentThroughput = MAX_BANDWIDTH;
			volumeFullReadIOPS = MAX_READ_IOPS / (numOfIO + numOfMigrating + outMigNum);
			volumeFullWriteIOPS = MAX_WRITE_IOPS / (numOfIO + numOfMigrating + outMigNum);
			volumeSpeed = currentThroughput / (numOfIO + numOfMigrating + outMigNum);
			availableVolumeSpeed = currentThroughput / (numOfIO + numOfMigrating + outMigNum + 1) ;
		}
	}
	
	public void checkVolumeStatus() {
		
		// remove all expired volumes
		Iterator<Volume> voIt = volumes.iterator();
		while (voIt.hasNext()) {
			Volume volume = (Volume) voIt.next();
			if (volume.checkExpire() && removeVolume(volume)) {
				voIt.remove();
				//logger.writeToEventLog("DELETE VOLUME " + volume.toString() + " at "+ this.toString());
				logger.writeToEventLog("DELETE VOLUME " + volume.toString() );
			}		
		}

/*		numOfIO = 0;
		numOfMigrating = 0;
		for (Volume v : volumes) {
			if (v.getStatus() == VolumeStatus.IO) {
				numOfIO ++;
			} else if (v.getStatus() == VolumeStatus.MIGRATING) {
				numOfMigrating ++;
			}
		}*/
		
		if (numOfIO + numOfMigrating != volumes.size()) {
			
			logger.writeToMigEventLog("VOLUME COUNT ERROR checkStatus at " + toString());
			//logger.writeToEventLog("VOLUME COUNT ERROR checkStatus at " + toString());
		}
		if (numOfMigrating < migratingTasks.size()) {
			String out = "";
			for (MigrationTask mTask : migratingTasks) {
				out += mTask.volume.getId() + " "; 
			}
			logger.writeToMigEventLog("VOLUME COUNT ERROR total mig < outMig " + out + " at " + toString());
		}
		// recalculate volume speed
		updateSpeed();

		if (numOfIO != 0 || numOfMigrating != 0) {

			SLAvio = 0;
			//validAllocatedCapacity = 0;
			for (Volume volume : volumes) {
/*				if (volume.getStatus() != VolumeStatus.MIGRATING && volume.getStatus() != VolumeStatus.INVALID) {
					validAllocatedCapacity += volume.getSize();
				}*/
				
				if (volume.getStatus() == VolumeStatus.IO || volume.getStatus() == VolumeStatus.MIGRATING) {
					volume.setSpeed(volumeSpeed);
					if (volume.checkSLAvio()) {
						SLAvio++;
					}
				} else {
					volume.setSpeed(0);
				}
			}
		}
		
		if (numOfMigrating != 0) {
			hasMigratingVolume = true;
		} else {
			hasMigratingVolume = false;
		}
		
		
	}
	
	public void checkMigration() {
		
		checkCapacityMigration();
		
		if (VolumeRequest.DIMENTION == 2) {
			checkIOPSMigration();
		}
	}
	
	public void doMigration() {
		
		if (Simulator.DEBUG)
			System.out.println("doMigration " + migratingTasks.size() + " " + toString());
		//logger.writeToMigEventLog("doMigration " + migratingTasks.size() + " " + toString());
		Iterator<MigrationTask> itMTask = migratingTasks.iterator();
		
		// iterate the migration list
		while (itMTask.hasNext()) {
			MigrationTask mTask = (MigrationTask) itMTask.next();
			//System.out.println(mTask.toString());
			// if the destination node is not selected
			// run default scheduling policy to choose on
			// add volume to the volume list of dest-node
			if (mTask.in == null) {
				if (mTask.selectDestNode()) {
					logger.writeToMigEventLog("MIG SCHEDULE SUCCESS mTask: " + mTask.toString() );
				}
			}
			
			boolean isDone;
			// if migration is done
			// remove volume from out node and recover previous volume state
			if (mTask.in != null ) {
				isDone = mTask.process();
				if (isDone) {
					logger.writeToMigEventLog("MIGRATION DONE " + mTask.toString());
					itMTask.remove();
					mTask.done();
					//logger.writeToMigEventLog("DONE REMOVED " + toString());
				} else {
					//logger.writeToMigEventLog("	MIGRATING " + mTask.toString());
				}
				
			} 
		}
	}
	
	// Algorithm 2
	private void checkCapacityMigration() {
		
		ArrayList<Volume> volumeList = new ArrayList<Volume>(volumes);

		// check capacity
		int capUtil = validAllocatedCapacity;
		int capBestFitUtil = MAX_CAPACITY;

		if (capUtil >= CAP_UP_TH) {
			// sort in decreasing order
			Collections.sort(volumeList, new Comparator<Volume>() {
				@Override
				public int compare(Volume o1, Volume o2) {
					return o2.getSize() - o1.getSize();
				}
			});
			
			// when current resource utilization is higher than UP threshold
			while (capUtil > CAP_UP_TH) {
				//System.out.println("CAP UTL TOO HIGH, MIGRATION NEEDED at " + toString());
				logger.writeToMigEventLog("CAP UTL TOO HIGH, MIGRATION NEEDED capUtil:" + capUtil + " " + CAP_UP_TH +" at " + toString() );
				//logger.writeToEventLog("CAP UTL TOO HIGH, MIGRATION NEEDED");
				
				Volume bestFitVolume = null;
				Iterator<Volume> itVr = volumeList.iterator();
				// find a volume to migrate
				while (itVr.hasNext()) {
					Volume volume = (Volume) itVr.next();
					
					int estTime = MigrationTask.calEstMigTime(volume, this) + Simulator.getTimer();
					if (volume.getStatus() == VolumeStatus.MIGRATING
							// the migration should be finished before volume dead
							|| estTime > volume.getExpireTime()) {
						itVr.remove();
						continue;
					}
										
					if (volume.getSize() >= (capUtil - Backend.CAP_UP_TH)) {
	
						int t = Backend.CAP_UP_TH - (capUtil - volume.getSize());
						if (t < capBestFitUtil) {
							capBestFitUtil = t;
							bestFitVolume = volume;
						} else {
							if (capBestFitUtil == Backend.MAX_CAPACITY) {
								bestFitVolume = volume;
								// break;
							}
							break;
						}
						/*capUtil = capUtil - bestFitVolume.getSize();
						migratingTasks.add(new MigrationTask(bestFitVolume, this));
						this.validAllocatedCapacity = this.validAllocatedCapacity - bestFitVolume.getSize();
						logger.writeToMigEventLog("ADD MIGRATE TASK, VOL: " + bestFitVolume.toString() + " from: " + this.toString());
						//bestFitVolume.setStatus(VolumeStatus.MIGRATING);
						this.changeVolumeStatus(bestFitVolume, VolumeStatus.MIGRATING);
						itVr.remove();*/
					}
					else {
						//logger.writeToMigEventLog("ADD FAIL Vol:" + volume.toString() + " estTime:" + estTime + " diff:" + (capUtil - Backend.CAP_UP_TH));
					}
				}
				
				if (volumeList.isEmpty()) {
					System.out.println("NO ELIGIABLE VOLUME TO MIGRATE");
					logger.writeToMigEventLog("NO ELIGIABLE VOLUME TO MIGRATE");
				}
				
				if (!volumeList.isEmpty() && bestFitVolume == null) {
					bestFitVolume = volumeList.get(0);
				}
				capUtil = capUtil - bestFitVolume.getSize();
				migratingTasks.add(new MigrationTask(bestFitVolume, this));
				this.validAllocatedCapacity = this.validAllocatedCapacity - bestFitVolume.getSize();
				logger.writeToMigEventLog("ADD MIGRATE TASK, VOL: " + bestFitVolume.toString() + " from: " + this.toString());
				//bestFitVolume.setStatus(VolumeStatus.MIGRATING);
				this.changeVolumeStatus(bestFitVolume, VolumeStatus.MIGRATING);
				itVr.remove();
			}
		}
		// if the utilization is too low, then migrate all volumes out and turn machine to sleep mode
		if (capUtil < Backend.CAP_DOWN_TH) {
			logger.writeToMigEventLog("CAP UTL TOO LOW, MIGRATION NEEDED at " + toString() + " validCap:" + validAllocatedCapacity);
			
			for (Volume volume : volumeList) {
				if (volume.getStatus() == VolumeStatus.MIGRATING 
						|| (MigrationTask.calEstMigTime(volume, this) + Simulator.getTimer() > volume.getExpireTime()))
					continue;
				migratingTasks.add(new MigrationTask(volume, this));
				this.validAllocatedCapacity = this.validAllocatedCapacity - volume.getSize();
				logger.writeToMigEventLog("ADD MIGRATE TASK, VOL: " + volume.toString() + " from: " + this.toString());
				this.changeVolumeStatus(volume, VolumeStatus.MIGRATING);
			}
		}
	}
	
	private void checkIOPSMigration() {
				
		ArrayList<Volume> volumeList = new ArrayList<Volume>(volumes);
		
		// volume with longest life goes first
		Collections.sort(volumeList, new Comparator<Volume>() {
			@Override
			public int compare(Volume o1, Volume o2) {
				return (o1.getExpireTime() > o2.getExpireTime()) ? -1:1;
			}
		});
		
		int ioUtil = currentThroughput;
		int ioBestFitUtil = MAX_BANDWIDTH;
		int i = 0;
		// if iops is too low, then we need to migrate
		while (ioUtil <= IOPS_DOWN_TH) {
			
			logger.writeToMigEventLog("IOPS UTL TOO LOW, MIGRATION NEEDED");
			System.out.println("IOPS UTL TOO LOW, MIGRATION NEEDED");
			Volume v = volumeList.get(i++);
			
			if (v.getStatus() == VolumeStatus.MIGRATING) {
				continue;
			}
			
			MigrationTask mTask = null;
			mTask = new MigrationTask(v, this);
			
			// if the estimate migration time is longer than volume lifetime
			// then don't migrate this volume and try next one
			while (mTask.estMigTime + mTask.startTime > mTask.volume.getExpireTime()) {
				if (i >= volumeList.size()) {
					mTask = null;
					break;
				}
				v = volumeList.get(i++);
				mTask = new MigrationTask(v, this);
			}
			
			if (mTask != null) {
				volumeList.remove(v);
				migratingTasks.add(mTask);
				this.validAllocatedCapacity = this.validAllocatedCapacity - v.getSize();
				logger.writeToMigEventLog("ADD MIGRATE TASK, VOL: " + v.toString() + " est:" + mTask.estMigTime + " remain:" + v.getExpireTime() + " from:" + this.toString());
				//v.setStatus(VolumeStatus.MIGRATING);
				this.changeVolumeStatus(v, VolumeStatus.MIGRATING);
			} else {
				// logical error!!
				// program should not goes here
				logger.writeToMigEventLog("CRITICAL ERROR!!!");
				break;
			}
			
			ioUtil = MAX_BANDWIDTH / volumeList.size();
		}
		
		if (ioUtil >= IOPS_UP_TH) {
			logger.writeToMigEventLog("IOPS UTL TOO HIGH, MIGRATION NEEDED");
			MigrationTask mTask = null;
			for (Volume volume : volumeList) {
				if (volume.getStatus() == VolumeStatus.MIGRATING)
					continue;
				mTask = new MigrationTask(volume, this);
				if (mTask.estMigTime + mTask.startTime < mTask.volume.getExpireTime()) {
					migratingTasks.add(mTask);
					this.validAllocatedCapacity = this.validAllocatedCapacity - volume.getSize();
					this.changeVolumeStatus(volume, VolumeStatus.MIGRATING);
					logger.writeToMigEventLog("ADD MIGRATE TASK, VOL: " + volume.toString() + " from: " + this.toString());
				}
			}
		}
		
	}
	
	public Volume createVolume(VolumeRequest vr) {
		
		Volume volume = new Volume(vr);
		volumes.offer(volume);
		allocatedCapacity += volume.getSize();
		validAllocatedCapacity += volume.getSize();
		//System.out.println("CREATE " + volume.toString() + " at " + Simulator.getTimer());
		logger.writeToEventLog("CREATE VOLUME VR: " + vr.toString() + " at "+ this.id);
		
		return volume;
	}
	
	public boolean removeHeadVolume() {
		
		logger.writeToEventLog("BEFOR DELETE VOLUME " + volumes.peek().toString() + " at "+ this.toString());
		Volume headVolume = volumes.poll();
		allocatedCapacity -= headVolume.getSize();
		
		if (headVolume.getStatus() == VolumeStatus.MIGRATING) {
			logger.writeToEventLog("DELETING MIGRATING VOLUME at " + this.id); 
		}
		
		changeVolumeStatus(headVolume, VolumeStatus.IDLE);
		//checkVolumeStatus();
			
		if (Simulator.DEBUG) {
			System.out.println("DELETE " + headVolume.toString() + " at " + Simulator.getTimer());
		}
		logger.writeToEventLog("DELETE VOLUME " + headVolume.toString() + " at "+ this.toString());
		
		return true;
	}
	
	public void changeVolumeStatus(Volume v, VolumeStatus status) {
		
		if (v.getStatus() == VolumeStatus.IDLE ) {	
			if (status == VolumeStatus.IO) {
				//logger.writeToMigEventLog("IDLE to IO numIO:" + numOfIO + "to" + (numOfIO+1)+ " Vid:" + v.getId() + " BKDid:" + this.id);
				numOfIO++;
			} else if (status == VolumeStatus.MIGRATING) {
				//logger.writeToMigEventLog("IDLE to MIG numMig:" + numOfMigrating + "to" + (numOfMigrating+1)+ " Vid:" + v.getId() + " BKDid:" + this.id);
				numOfMigrating ++;
			}
		} else if (v.getStatus() == VolumeStatus.IO) {
			if (status == VolumeStatus.IDLE) {
				//logger.writeToMigEventLog("IO to IDLE numIO:" + numOfIO + "to" + (numOfIO-1)+ " Vid:" + v.getId() + " BKDid:" + this.id);
				numOfIO--;
			} else if (status == VolumeStatus.MIGRATING) {
				//logger.writeToMigEventLog("IO to MIG numIO:" + numOfIO + "to" + (numOfIO-1) + " numMig:" + numOfMigrating + "to" + (numOfMigrating+1)+ " Vid:" + v.getId() + " BKDid:" + this.id);
				numOfIO--;
				numOfMigrating ++;
			}
		} else if (v.getStatus() == VolumeStatus.MIGRATING) {
			if (status == VolumeStatus.IDLE) {
				//logger.writeToMigEventLog("MIG to IDLE numMig:" + numOfMigrating + "to" + (numOfMigrating-1)+ " Vid:" + v.getId() + " BKDid:" + this.id);
				numOfMigrating --;
			} else if (status == VolumeStatus.IO) {
				//logger.writeToMigEventLog("MIG to IO numIO:" + numOfIO + "to" + (numOfIO+1) + " numMig:" + numOfMigrating + "to" + (numOfMigrating-1)+ " Vid:" + v.getId() + " BKDid:" + this.id);
				numOfMigrating--;
				numOfIO++;
			}
		}
		v.setStatus(status);
		updateSpeed();
	}

	@Override
	public String toString() {
	//	if (Simulator.DEBUG) {
			String IOvolumeString = "";
			String migrationString = "";
			String idleVolumeString = "";
			String mTasksString = "";
			for (Volume v : volumes) {
				if (v.getStatus() == VolumeStatus.IO) {
					IOvolumeString += v.getId() + " ";
				} else if (v.getStatus() == VolumeStatus.MIGRATING) {
					migrationString += v.getId() + " ";
				} else {
					idleVolumeString += v.getId() + " ";
				}
			}
			for (MigrationTask mTask : migratingTasks) {
				mTasksString += mTask.volume.getId() + " ";
			}
			return "BKD " + id + 
					" " + numOfIO + "IO: " + IOvolumeString + 
					" " + numOfMigrating + "M:" + migrationString +
					" idle: "+ idleVolumeString + 
					" mTask:" + mTasksString +
					" volSpd=" + Integer.toString(volumeSpeed) +
					" avlSpd=" + Integer.toString(availableVolumeSpeed); 
/*		} else {
			return "BKD " + id + 
					" " + numOfIO + "IO " +
					" " + numOfMigrating + "M " +
					" volSpd=" + Integer.toString(volumeSpeed) +
					" avlSpd=" + Integer.toString(availableVolumeSpeed); 
		}*/
		
	}
	
	public int calWeight() {
		if (VolumeRequest.DIMENTION == 1) {
			return Backend.MAX_CAPACITY - allocatedCapacity;
		} else if (VolumeRequest.DIMENTION == 2) {
			return (int)(100 * ((double)(Backend.MAX_CAPACITY - allocatedCapacity) / Backend.MAX_CAPACITY + (double)availableVolumeSpeed / Backend.MAX_BANDWIDTH));
		} else {
			return 0;
		}
	}
	
	public boolean hasActiveVolume() {
		return !volumes.isEmpty();
	}
	
	
	public int getSLAvio() {
		return SLAvio;
	}

	public static void resetIdGen() {
		idGen = 0;
	}
	public int getCurrentThroughput() {
		return currentThroughput;
	}

	public int getAllocatedCapacity() {
		return allocatedCapacity;
	}

	public int getVolumeSpeed() {
		return volumeSpeed;
	}

	public PriorityQueue<Volume> getVolumes() {
		return volumes;
	}

	public int getAvailableVolumeSpeed() {
		return availableVolumeSpeed;
	}

	public int getId() {
		return id;
	}

	
	public ArrayList<MigrationTask> getMigratingTasks() {
		return migratingTasks;
	}


	@Override
	public int compareTo(Backend arg0) {
		if (arg0 instanceof Backend) {
			// sort allocated space in decreasing order 
			return (weight > arg0.weight) ? -1:1;
		}
		return 0;
	}
	
}
