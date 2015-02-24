package simulator;

import java.util.ArrayList;

import simulator.Scheduler.ScheduleMethod;

public class Simulator {

	public static final int SIMULATION_TIME = 300000;
	public static int NODES_NUM = 560;
	
	public static final boolean DEBUG = false;
	public static int timer = 0;
	
	private static ArrayList<Backend> backendList;
	private Logger logger;
	private Scheduler scheduler;
	private LoadReader loadReader;
	
	public Simulator(int numOfNodes, ScheduleMethod method) {

		Logger.numOfNodes = Integer.toString(NODES_NUM);
		logger = Logger.getLoggerInstance();
		
		Backend.resetIdGen();
		resetTimer();
		
		if (method == ScheduleMethod.DEFAULT) {
			scheduler = new DefaultScheduler();
			logger.initLog("DEFAULT");
		} else if (method == ScheduleMethod.FFD) {
			scheduler = new FFDScheduler();
			logger.initLog("VBP_FFD");
		}
		
		backendList = new ArrayList<Backend>(numOfNodes);
		
		for (int i = 0; i < numOfNodes; i++) {
			Backend backend = new Backend();
			backendList.add(backend);
		}
		
		loadReader = new LoadReader();
	}
	
	public static void main(String[] args) {
		
		//System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		System.out.println("start");
		NODES_NUM = 2000;
		/*Simulator simulator = new Simulator(NODES_NUM, ScheduleMethod.FFD);
		simulator.runSimulation();*/
		//simulator = new Simulator(NODES_NUM, ScheduleMethod.FFD);
		
/*		VolumeRequest vr = new VolumeRequest(100, 0, 1000000, 3600, 300);
		Backend backend = Simulator.backendList.get(0);
		Volume v = null;
		for (int i = 1; i <= 17; i++) {
			v = backend.createVolume(vr);
			backend.readVolume(v);
			vr.setId(100 + i);
		}
		*/
		
		//simulator.runSimulation();
	
		for (int i = 1000; i <= 1050; i = i + 50) {
			Simulator.NODES_NUM = i;
			System.out.println("start " + i);
			Simulator simulator = new Simulator(NODES_NUM,
					ScheduleMethod.DEFAULT);
			simulator.runSimulation();

			simulator = new Simulator(NODES_NUM, ScheduleMethod.FFD);
			simulator.runSimulation();
			System.out.println("done " + i);
		}
		
		System.out.println("end " + timer);
	}

	public void runSimulation() {
		
		logger.writeToEventLog("Simulation Start!");
		VolumeRequest vr = null;
		vr = loadReader.readOneRequest();
		int numofrequests = 1;
		
		while (timer < Logger.LOG_END) {
			System.out.println(NODES_NUM + " " + timer);
			// If there is a new request whose arrival time is right now
			while (vr != null && vr.getArrivalTime() == timer) {
				// add request to scheduler's queue
				scheduler.addNewRequestToQ(vr);
			
				//if (scheduler.method == ScheduleMethod.DEFAULT && (!scheduler.isQEmpty())) {
				if (scheduler.method == ScheduleMethod.DEFAULT) {
					scheduler.schedule(backendList);
				}
				// read next request
				vr = loadReader.readOneRequest();
/*				if (timer >= Logger.LOG_START)
					numofrequests++;*/
			}
			// If there is a request waiting in the queue
			if (scheduler.method == ScheduleMethod.FFD && (!scheduler.isQEmpty())
					&& ((timer & 1) != 0)) {
				// run scheduling algorithm 
				scheduler.schedule(backendList);
			}
			
			scheduler.volumeCheck(backendList);
			
/*			if (timer >= Logger.LOG_START) {
				scheduler.migrationCheck(backendList);
				scheduler.migration(backendList);
			}*/
			
			//vr = loadReader.readOneRequest();
			timer++;
		}
		
		//System.out.println(numofrequests);
		logger.writeToEventLog("Simulation Done!");
		//System.out.println(scheduler.failCap);
		//logger.writeToFailCapLog(Integer.toString(scheduler.failCap));
		logger.closeFile();
	}

	public static void resetTimer() {
		timer = 0;
	}
	
	public static int getTimer() {
		return timer;
	}

	public static ArrayList<Backend> getBackendList() {
		return backendList;
	}
	
	
}
