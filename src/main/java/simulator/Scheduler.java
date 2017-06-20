package simulator;

import java.util.ArrayList;
import java.util.LinkedList;

public abstract class Scheduler {

	public static enum ScheduleMethod {
		DEFAULT, FFD , HIOBS , THR
	}

	public static final int MAX_Q_LENGTH = 100;

	ScheduleMethod method;
	public static Logger logger;
    LinkedList<VolumeRequest> requestQueue;
    static ArrayList<Backend> candidateList;
    public int numOfNoCandidate;

	public Scheduler(ScheduleMethod _method) {
		this.method = _method;
		logger = Logger.getLoggerInstance();
	}

	public abstract void addNewRequestToQ(VolumeRequest vr);

	public abstract void schedule(ArrayList<Backend> backendList);

	public abstract boolean isQEmpty();

	public void volumeCheck(ArrayList<Backend> backendList) {

	    int simulation_time = Simulator.getTimer();

		if (simulation_time >= Logger.LOG_START && simulation_time <= Logger.LOG_END) {
			StringBuffer volSpdStringBuffer = new StringBuffer();
			StringBuffer sLAvioStringBuffer = new StringBuffer();
			StringBuffer volNumStringBuffer = new StringBuffer();
			StringBuffer availSpdStringBuffer = new StringBuffer();

			volSpdStringBuffer.append(simulation_time);
			sLAvioStringBuffer.append(simulation_time);
			volNumStringBuffer.append(simulation_time);
			availSpdStringBuffer.append(simulation_time);

			for (Backend b : backendList) {
				b.checkVolumeStatus();

				// output metric results
				if (b.hasActiveVolume()) {
					volSpdStringBuffer.append("," + b.getVolumeSpeed());
				} else {
					volSpdStringBuffer.append(",NaN");
				}

				availSpdStringBuffer.append("," + b.getAvailableVolumeSpeed());
                sLAvioStringBuffer.append("," + b.getSLAvio());

				//utlString += "," + b.getAllocatedCapacity();
				volNumStringBuffer.append("," + b.getVolumes().size());
			}

			logger.writeToVolSpdLog(volSpdStringBuffer.toString());
			logger.writeToSLALog(sLAvioStringBuffer.toString());

			//logger.writeToUtlLog(utlString);
			logger.writeToVolNumLog(volNumStringBuffer.toString());
			logger.writeToAvailSpdLog(availSpdStringBuffer.toString());

		} else {
			for (Backend b : backendList) {
				b.checkVolumeStatus();
			}
		}
	}

//	public void migrationCheck(ArrayList<Backend> backendList) {
//
//		for (Backend backend : backendList) {
//			if (!backend.getVolumes().isEmpty()) {
//				backend.checkMigration();
//			}
//		}
//	}

//	public void migration(ArrayList<Backend> backendList) {
//
//		for (Backend backend : backendList) {
//			if (!backend.getMigratingTasks().isEmpty()) {
//				backend.doMigration();
//			}
//		}
//	}

}
