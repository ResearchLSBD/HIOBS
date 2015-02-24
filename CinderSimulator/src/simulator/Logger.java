package simulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {
	
	//public static final int LOG_START = 0;
	public static int LOG_START;
	public static int LOG_END;

	public static final String VOLUME_SPD_FILE= "VolumeSpeed.csv";
	public static final String SLA_VIO_FILE = "SLAvio.csv";
	public static final String EVENT_LOG_FILE = "eventLog.txt";
	public static final String UTL_FILE = "utl.csv";
	public static final String FAIL_CAP_FILE = "failCap.txt";
	public static final String VOLUME_NUM_FILE = "VolumeNum.csv";
	public static final String MIGRATION_FILE = "mig_event.txt";
	public static final String AVAIL_SPD_FILE  = "availSpd.csv";
	
	private static boolean EN_VOLUME_SPD;
	private static boolean EN_VOLUME_NUM;
	private static boolean EN_SLA_VIO;
	private static boolean EN_EVENT;
	private static boolean EN_UTL;
	private static boolean EN_FAIL_CAP;
	private static boolean EN_MIG_EVENT;
	private static boolean EN_AVAIL_SPD;
	
	File volSpdFile;
	File sLAvioFile;
	File eventLogFile;
	File utlFile;
	File failCapFile;
	File volNumFile;
	File migEventFile;
	File availSpdFile;
	
	BufferedWriter volSpdWriter;
	BufferedWriter slavioWriter;
	BufferedWriter eventLogWriter;
	BufferedWriter utlWriter;
	BufferedWriter failCapWriter;
	BufferedWriter volNumwWriter;
	BufferedWriter migEventWriter;
	BufferedWriter availSpdWriter;
	//private String name;
	public static String numOfNodes = Integer.toString(Simulator.NODES_NUM);
	public String scheduling;
	
	private static Logger instance = new Logger();
	
	public static Logger getLoggerInstance() {
		return instance;
	}

	private Logger() {
		if (Simulator.DEBUG == true) {
			LOG_START = 0;
			LOG_END = Simulator.SIMULATION_TIME;
		} else {
			LOG_START = 43200; //12th hour
			LOG_END = 216000; // 60th hour
		}
		
		EN_EVENT = true;
		EN_FAIL_CAP = false;
		EN_SLA_VIO = true;
		EN_UTL = false;
		EN_VOLUME_NUM = true;
		EN_VOLUME_SPD = true;
		EN_MIG_EVENT = false;
		EN_AVAIL_SPD = true;
	}
	
	public void initLog(String sche) {
		volSpdFile = new File( numOfNodes + "_" + sche + "_" + VOLUME_SPD_FILE);
		sLAvioFile = new File( numOfNodes + "_" + sche + "_" + SLA_VIO_FILE);
		eventLogFile = new File( numOfNodes + "_" + sche + "_" + EVENT_LOG_FILE);
		utlFile = new File(numOfNodes + "_" + sche + "_" + UTL_FILE);
		failCapFile = new File(numOfNodes + "_" + sche + "_" + FAIL_CAP_FILE);
		volNumFile = new File(numOfNodes + "_" + sche + "_" + VOLUME_NUM_FILE);
		migEventFile = new File(numOfNodes + "_" + sche + "_" + MIGRATION_FILE);
		availSpdFile = new File(numOfNodes + "_" + sche + "_" + AVAIL_SPD_FILE);
		
		openFiles(volSpdFile);
		openFiles(sLAvioFile);
		openFiles(eventLogFile);
		openFiles(utlFile);
		openFiles(failCapFile);
		openFiles(volNumFile);
		openFiles(migEventFile);
		openFiles(availSpdFile);
		
		try {
			volSpdWriter = new BufferedWriter(new FileWriter(volSpdFile));
			slavioWriter = new BufferedWriter(new FileWriter(sLAvioFile));
			eventLogWriter = new BufferedWriter(new FileWriter(eventLogFile));
			utlWriter = new BufferedWriter(new FileWriter(utlFile));
			failCapWriter = new BufferedWriter(new FileWriter(failCapFile));
			volNumwWriter = new BufferedWriter(new FileWriter(volNumFile));
			migEventWriter = new BufferedWriter(new FileWriter(migEventFile));
			availSpdWriter = new BufferedWriter(new FileWriter(availSpdFile));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("logger create fail");
		}
	}
	
	private void openFiles(File filename) {
		try {
			if (filename.exists()) {
				filename.delete();
			}
			filename.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeToAvailSpdLog(String str) {
		if (EN_AVAIL_SPD && Simulator.getTimer() >= LOG_START && Simulator.getTimer() <= LOG_END)
			writeLine(availSpdWriter, str);
	}
	
	public void writeToMigEventLog(String str) {
		if (EN_MIG_EVENT && Simulator.getTimer() >= LOG_START && Simulator.getTimer() <= LOG_END)
			writeLine(migEventWriter, Integer.toString(Simulator.timer) + " "+ str);
	}
	public void writeToVolNumLog(String str) {
		if (EN_VOLUME_NUM && Simulator.getTimer() > LOG_START && Simulator.getTimer() <= LOG_END)
			writeLine(volNumwWriter, str);
	}
	
	public void writeToVolSpdLog(String str) {
		if (EN_VOLUME_SPD && Simulator.getTimer() > LOG_START && Simulator.getTimer() <= LOG_END)
			//writeLine(volSpdWriter, Integer.toString(Simulator.timer) + " "+ str);
			writeLine(volSpdWriter, str);
	}
	
	public void writeToSLALog(String str) {
		if (EN_SLA_VIO && Simulator.getTimer() > LOG_START && Simulator.getTimer() <= LOG_END)
			//writeLine(slavioWriter, Integer.toString(Simulator.timer) + " "+str);
			writeLine(slavioWriter, str);
	}
	
	public void writeToEventLog(String str) {
		if (EN_EVENT && Simulator.getTimer() >= LOG_START && Simulator.getTimer() <= LOG_END)
			writeLine(eventLogWriter, Integer.toString(Simulator.timer) + " "+str);
	}
	
	public void writeToUtlLog(String str) {
		if (EN_UTL && Simulator.getTimer() >= LOG_START && Simulator.getTimer() <= LOG_END)
			writeLine(utlWriter, Integer.toString(Simulator.timer) + " "+str);
	}
	public void writeToFailCapLog(String str) {
		if (EN_FAIL_CAP && Simulator.getTimer() >= LOG_START && Simulator.getTimer() <= LOG_END)
			writeLine(failCapWriter, Integer.toString(Simulator.timer) + " "+str);
	}
	
	private void writeLine(BufferedWriter writer, String line) {
		try {
			writer.write(line + "\r\n");
			writer.flush();
		} catch (Exception e) {
			System.out.println(line);
			e.printStackTrace();
		}
	}
	
	public void closeFile() {
		try {
			volSpdWriter.close();
			slavioWriter.close();
			eventLogWriter.close();
			utlWriter.close();
			failCapWriter.close();
			volNumwWriter.close();
			migEventWriter.close();
			availSpdWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setScheduling(String scheduling) {
		this.scheduling = scheduling;
	}
	
}
