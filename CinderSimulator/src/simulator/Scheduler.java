package simulator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

import simulator.Volume.VolumeStatus;

public abstract class Scheduler {

	public int count = 20;
	public static enum ScheduleMethod {
		DEFAULT, FFD
	};

	public static final int MAX_Q_LENGTH = 100;

	ScheduleMethod method;
	public int failCap = 10;
	public Logger logger;

	public Scheduler(ScheduleMethod _method) {
		this.method = _method;
		logger = Logger.getLoggerInstance();
	}

	public abstract void addNewRequestToQ(VolumeRequest vr);

	public abstract void schedule(ArrayList<Backend> backendList);

	public abstract boolean isQEmpty();

	public void volumeCheck(ArrayList<Backend> backendList) {
		
		if (count == 20 && Simulator.getTimer() >= Logger.LOG_START) {
			StringBuffer volSpdStringBuffer = new StringBuffer();
			
			StringBuffer sLAvioStringBuffer = new StringBuffer();
			//String utlString = "";
			StringBuffer volNumStringBuffer = new StringBuffer();
			StringBuffer availSpdStringBuffer = new StringBuffer();
			
			for (Backend b : backendList) {
				
				b.checkVolumeStatus();
				if (b.hasActiveVolume()) {
					volSpdStringBuffer.append("," + b.getVolumeSpeed());
				} else {
					volSpdStringBuffer.append(",NaN");
				}
				availSpdStringBuffer.append("," + b.getAvailableVolumeSpeed());
				if (method == ScheduleMethod.DEFAULT) {
					sLAvioStringBuffer.append(","+b.getSLAvio());
				}
				//utlString += "," + b.getAllocatedCapacity();
				volNumStringBuffer.append("," + b.getVolumes().size());
			}
			logger.writeToVolSpdLog(volSpdStringBuffer.toString());
			if (method == ScheduleMethod.DEFAULT) {
				logger.writeToSLALog(sLAvioStringBuffer.toString());
			}
			//logger.writeToUtlLog(utlString);
			logger.writeToVolNumLog(volNumStringBuffer.toString());
			logger.writeToAvailSpdLog(availSpdStringBuffer.toString());
			
			count = 0;
		} else {
			for (Backend b : backendList) {
				
				b.checkVolumeStatus();
			}
			if (count >= 20) {
				count = 0;
			} else {
				count ++;
			}
		}
	}

	public void migrationCheck(ArrayList<Backend> backendList) {

		for (Backend backend : backendList) {
			if (!backend.getVolumes().isEmpty()) {
				backend.checkMigration();
			}
		}
	}

	public void migration(ArrayList<Backend> backendList) {
		
		for (Backend backend : backendList) {
			if (!backend.getMigratingTasks().isEmpty()) {
				backend.doMigration();
			}
		}
	}

}
