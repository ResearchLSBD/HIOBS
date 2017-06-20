package simulator;

import simulator.Volume.VolumeStatus;

public class MigrationTask {

	Volume volume;
	Backend out;
	Backend in;
	
	public int startTime;
	public int finishTime;
	public int estMigTime;
	
	private int doneSize;
	VolumeStatus statusSave;
	
	public MigrationTask(Volume v, Backend _out) {
		this.volume = v;
		this.out = _out;
		this.in = null;
		finishTime = -1;
		startTime = Simulator.getTimer();
		estMigTime = calEstMigTime(v, _out);
		doneSize = 0;
		statusSave = v.getStatus();
	}
	
	public boolean process() {
		if (doneSize <= volume.getSize()) {
			copyData();
			return false;
		} else {
			finishTime = Simulator.getTimer();
			return true;
		}
	}
	
	// if volume expires during migration
	// try to remove volume on in-node list
	public void volumeExpire() {
		try {
			if (in != null && in.getVolumes().contains(volume)) {
			//if (in != null) {
				in.getVolumes().remove(volume);
				in.numOfMigrating -- ;
				in.allocatedCapacity = in.allocatedCapacity - volume.getSize();
				in.validAllocatedCapacity = in.validAllocatedCapacity - volume.getSize();
			}
		} catch (Exception e) {
			System.out.println(this.toString());
			System.out.println(in.getVolumes().size());
		}
		
	}
	
	public void recoverStatus() {
		volume.setStatus(statusSave);
	}
	
	// Note: the speed of migration volume may change dynamically
	// so this function estimate migration time based on current volume speed on host node 
	public static int calEstMigTime(Volume v, Backend out) {
		
		return v.getSize() / out.volumeFullReadIOPS;
	}
	
//	public boolean selectDestNode() {
//		in = DefaultScheduler.migrationSchedule(this);
//
//		if (in != null) {
//			in.getVolumes().offer(volume);
//			in.numOfMigrating++;
//			in.allocatedCapacity += volume.getSize();
//			in.validAllocatedCapacity += volume.getSize();
//			in.updateSpeed();
//			return true;
//		} else {
//			return false;
//		}
//	}
	
	public void done() {
		out.getVolumes().remove(volume);
		out.numOfMigrating--;
		out.allocatedCapacity = out.allocatedCapacity - volume.getSize();
		out.checkVolumeStatus();
		in.changeVolumeStatus(volume, statusSave);
	}
	
	private void copyData() {
		if (in.volumeFullWriteIOPS <= out.volumeFullReadIOPS) {
			doneSize += in.volumeFullWriteIOPS;
		} else {
			doneSize += out.volumeFullReadIOPS;
		}
	}
	
	@Override
	public String toString() {
		String inId;
		
		if (in == null) {
			inId = "N/A";
		} else {
			inId = in.toString();
		}
		return "task start:" + startTime + " finish:" + finishTime 
				+ " Vol:" + volume.getId() + " data:" + doneSize + " total:" + volume.getSize()
				+ "\n	from:" + out.toString()+ "\n	to:" + inId;
	}
}
