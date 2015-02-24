package simulator;

public class Volume implements Comparable<Volume>{
		
	public static enum VolumeStatus { INVALID, IDLE, IO, MIGRATING };
	
	private int id;
	private VolumeStatus status;
	private int size;
	private int expireTime;
	private int speed;
	private int SLA;
	
	public boolean SLAvio = false;
	
	private static int idGen = 0;
	
	public Volume() {
		size = -1;
		expireTime = -1;
		status = VolumeStatus.INVALID;
	}
	
	public Volume(VolumeRequest vr) {
		this.size = vr.getSize();
		this.expireTime = Simulator.getTimer() + vr.getDuration();
		this.SLA = vr.getSLA();
		this.status = VolumeStatus.IDLE;
		this.id = vr.getId();
	}
	
	public Volume(int size, int expireTime) {
		this.size = size;
		this.expireTime = expireTime;
		this.status = VolumeStatus.IDLE;
		this.id = idGen++;
	}
	
	public boolean checkExpire() {
		
		if (expireTime <= Simulator.getTimer())
			return true;
		return false;
	}
	
	public boolean checkSLAvio() {
		
		if (speed != 0 && speed < SLA) {
			SLAvio = true;
		} else {
			SLAvio = false;
		}
		
		return this.SLAvio;
	}
	
	@Override
	public String toString() {
		String statusString;
		if (status == VolumeStatus.IO) {
			statusString = "IO";
		} else if (status == VolumeStatus.MIGRATING) {
			statusString = "MIG";
		} else {
			statusString = "IDLE";
		}
		
		return id + " size:" + size + " expire:" + expireTime + " " + statusString;
	}
	
	@Override
	public int compareTo(Volume o) {
		if (o instanceof Volume) {
			// sort increasing
			return (expireTime > o.getExpireTime()) ? 1:-1;
		}
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Volume v = (Volume) obj;
		return (this.getId() == v.getId());
	}
	
	public VolumeStatus getStatus() {
		return status;
	}
	public void setStatus(VolumeStatus status) {
		this.status = status;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getExpireTime() {
		return expireTime;
	}
	public void setExpireTime(int expireTime) {
		this.expireTime = expireTime;
	}
	public int getSpeed() {
		return speed;
	}
	public void setSpeed(int speed) {
		this.speed = speed;
	}

	public int getSLA() {
		return SLA;
	}

	public int getId() {
		return id;
	}


	
}
