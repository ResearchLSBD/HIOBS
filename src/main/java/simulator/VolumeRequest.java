package simulator;

public class VolumeRequest implements Comparable<VolumeRequest>{
	
	public static int DIMENTION = 2;
	
	private int id;
	private int size;
	private int duration;
	private int SLA;
	private int arrivalTime;
	private int weight;
	private int wait;
	public boolean served = false;
	
	private static int idGen = 0;
	//private static int sizeCoefficient = 1;
	//private static int SLACoefficient = 1;
	
	public VolumeRequest(int id, int arrivalTime, int size, int duration, int sLA) {
		this.id = id;
		this.size = size;
		this.duration = duration;
		SLA = sLA;
		this.arrivalTime = arrivalTime;
		weight = calWeight();
		wait = 0;
	}
	
	private int calWeight() {

		if (this.DIMENTION == 1) {
			//return size * sizeCoefficient;
			return size;
		} else if (this.DIMENTION == 2){
			//return size * sizeCoefficient + SLA * SLACoefficient;
			return (int)(100 * ((double)size / new Backend().MAX_CAPACITY + (double)SLA / new Backend().MAX_BANDWIDTH));
		} else {
			return 0;
		}

	}
	
	//@Override
	public int compareTo(VolumeRequest arg0) {
		if (arg0 instanceof VolumeRequest) {
			// sort decreasing
			return (weight > arg0.weight) ? -1:1;
		}
		return 0;
	}
	
	@Override
	public String toString() {
		return id + " S" + size + " D" + duration + " A" + SLA + " W" + weight;
	}
	
	public int getWait() {
		return wait;
	}

	public void increaseWait() {
		this.wait ++;
	}

	public int getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(int arrivalTime) {
		this.arrivalTime = arrivalTime;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public int getSLA() {
		return SLA;
	}

	public void setSLA(int sLA) {
		SLA = sLA;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
}
