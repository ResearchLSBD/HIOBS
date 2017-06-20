package simulator;

import com.sun.deploy.util.StringUtils;
import simulator.Scheduler.ScheduleMethod;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Simulator {
	public static int SIMULATION_TIME = 120000;
	public static int NUMBER_OF_BACKENDS = 8;
	public static int ITERATION_NUM = 10;
	
	public static final boolean DEBUG = false;
	public static int timer = 0;
	
	private static ArrayList<Backend> backendList;
	private static Logger logger;
	private Scheduler scheduler;
	private LoadReader loadReader;

	private static String CONFIG_FILE = "config.json";


	public Simulator(ScheduleMethod method) {

		Logger.numOfNodes = Integer.toString(NUMBER_OF_BACKENDS);
		logger = Logger.getLoggerInstance();
		
		Backend.resetIdGen();
		resetTimer();

        if (method == ScheduleMethod.DEFAULT) {
            scheduler = new DefaultScheduler();
            logger.initLog("DEFAULT");
        } else if (method == ScheduleMethod.HIOBS) {
            scheduler = new HIOBSScheduler();
            logger.initLog("HIOBS");
        } else if (method == ScheduleMethod.THR) {
            scheduler = new ThrScheduler();
            logger.initLog("THR");
        }

		loadReader = new LoadReader();
	}

	public static void main(String[] args) {
        JSONArray schedulerList = null;
        JSONArray backendConfigList = null;
        JSONObject backendsJSON = null;
        JSONObject loadGeneratorJSON = null;
        String argsToLoadGenerator[] = null;
        JSONArray numberOfNodeList = null;

        // Read and parse config.json file
        try {
            JSONObject simulatorJSON = (JSONObject) (new JSONParser()).parse(new FileReader(CONFIG_FILE));
            JSONObject loggerJSON = (JSONObject) simulatorJSON.get("LOGGER");
            loadGeneratorJSON = (JSONObject) simulatorJSON.get("LOAD_GENERATOR");
            backendsJSON = (JSONObject) simulatorJSON.get("BACKENDS");

            // Parse simulation parameters
            Long simulation_time = (Long) simulatorJSON.get("SIMULATION_TIME");
            Long numberOfIterations = (Long) simulatorJSON.get("NUMBER_OF_ITERATIONS");
            schedulerList = (JSONArray) simulatorJSON.get("SCHEDULER_LIST");

            // Parse logg parameters
            Long logStart = (Long) loggerJSON.get("LOG_START");
            Long logEnd = (Long) loggerJSON.get("LOG_END");
            String logOutputDir = (String) loggerJSON.get("LOG_OUTPUT_DIR");

            // Parse LoadGenerator parameters
            argsToLoadGenerator = new String[5];
            argsToLoadGenerator[0] = loadGeneratorJSON.get("NUMBER_OF_REQUESTS").toString();
            argsToLoadGenerator[1] = loadGeneratorJSON.get("MEAN_ARRIVAL_TIME").toString();
            argsToLoadGenerator[2] = loadGeneratorJSON.get("MEAN_EXPIRATION_TIME").toString();

            List<String> volumeSizeStringList = new ArrayList<String>();
            JSONArray volumeSizeJSONList = (JSONArray) loadGeneratorJSON.get("VOLUME_SIZE_LIST");
            Iterator<Long> tempIterator = volumeSizeJSONList.iterator();
            while (tempIterator.hasNext()) {
                volumeSizeStringList.add(tempIterator.next().toString());
            }
            argsToLoadGenerator[3] = StringUtils.join(volumeSizeStringList, ",");

            List<String> slaStringList = new ArrayList<String>();
            JSONArray slaSizeJSONList = (JSONArray) loadGeneratorJSON.get("SLA_LIST");
            tempIterator = slaSizeJSONList.iterator();
            while (tempIterator.hasNext()) {
               slaStringList.add(tempIterator.next().toString());
            }
            argsToLoadGenerator[4] = StringUtils.join(slaStringList, ",");

            // Parse Backend parameters
            backendConfigList = (JSONArray) backendsJSON.get("CLASSES");
            numberOfNodeList = (JSONArray) backendsJSON.get("NUMBER_OF_BACKEND_LIST");

            // Set up Simulator
            Simulator.SIMULATION_TIME = simulation_time.intValue();
            Simulator.ITERATION_NUM = numberOfIterations.intValue();

            // Set up Logger
            Logger loggerRef = Logger.getLoggerInstance();
            if(!Simulator.DEBUG) {
                loggerRef.LOG_START = logStart.intValue();
                loggerRef.LOG_END = logEnd.intValue();
            }

            loggerRef.LOG_OUTPUT_DIR = logOutputDir;

            System.out.println(" ----------------------------- Configuration ---------------------------- \n");
            System.out.println("Simulation time: " + simulation_time);
            System.out.println("Number of iterations: " + numberOfIterations);
            System.out.println("Log start: " + logStart);
            System.out.println("Log end: " + logEnd);
            System.out.println("Log ouput directory: " + logOutputDir);
            System.out.println("Number of requests: " + argsToLoadGenerator[0]);
            System.out.println("Mean arrival time: " + argsToLoadGenerator[1]);
            System.out.println("Mean expiration time: " + argsToLoadGenerator[2]);
            System.out.println("Volume size list: " + volumeSizeJSONList);
            System.out.println("SLA list: " + slaStringList);
            System.out.println("Scheduler list: " + schedulerList);

            System.out.println("Backend list:");
            Iterator<JSONObject> objIterator = backendConfigList.iterator();
            int j = 1;
            while (objIterator.hasNext()) {
                System.out.println("\tBackend " + j + " : " + objIterator.next());
                j++;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Run simulations
        System.out.println("\n ---------------------------- Simulation ------------------------------- \n");
        Iterator<Long> numberOfBackends = numberOfNodeList.iterator();
        while (numberOfBackends.hasNext()) {
            Simulator.NUMBER_OF_BACKENDS = numberOfBackends.next().intValue();
            System.out.println("Number of backends: " + Simulator.NUMBER_OF_BACKENDS);

            Iterator<String> iterator = schedulerList.iterator();
            while (iterator.hasNext()) {
                // Create a initial request input file
                LoadGenerator.main(argsToLoadGenerator);

                String schedulerName = iterator.next();
                Simulator simulator = new Simulator(ScheduleMethod.valueOf(schedulerName));
                simulator.backendList = Simulator.createBackendList(backendsJSON);

                System.out.print("\rStarted scheduler: " + schedulerName);

                // Run simulations
                for (int i = 0; i < Simulator.ITERATION_NUM; i++) {
                    System.out.printf("\rStarted scheduler: %s [%d/%d  %.1f%%]", schedulerName, i,
                            Simulator.ITERATION_NUM, (((float)i)/((float) Simulator.ITERATION_NUM)) * 100.0f);

                    simulator.runSimulation();

                    // Create a new request input file
                    LoadGenerator.main(argsToLoadGenerator);

                    // Reset simulator
                    simulator.resetSimulator();
                }

                simulator.logger.closeFile();

                System.out.print("\rFinished scheduler: " + schedulerName + "\n");
            }

            System.out.println("");
        }

        // Make a copy of config.json file to output directory
        try {
            File source = new File(CONFIG_FILE);
            File target = new File(Simulator.logger.LOG_OUTPUT_DIR + "/" + CONFIG_FILE);

            if(!source.exists()) {
                new Exception("Error: Configuration file doesn't exist");
            }

            Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
	}

	public void runSimulation() {
		logger.writeToEventLog("Simulation Start!");
		VolumeRequest vr = loadReader.readOneRequest();

		while (timer < Simulator.SIMULATION_TIME) {
			// If there is a new request whose arrival time is right now
			while (vr != null && vr.getArrivalTime() == timer) {
				// add request to scheduler's queue
				scheduler.addNewRequestToQ(vr);
			
				if (!scheduler.isQEmpty()) {
					scheduler.schedule(backendList);
				}

				vr = loadReader.readOneRequest();
			}

			scheduler.volumeCheck(backendList);
			timer++;
		}

		logger.writeToEventLog("Simulation Done!");
	}

	public static void resetTimer() {
		timer = 0;
	}
	
	public static int getTimer() {
		return timer;
	}

	public static ArrayList<Backend> createBackendList(JSONObject backendsJSON) {
        ArrayList<Backend> backendList = null;

	    try {
	        float totalNumberOfBackEnds = Simulator.NUMBER_OF_BACKENDS;
            JSONArray backendConfigList = (JSONArray) backendsJSON.get("CLASSES");

            // Check proportion of backends
            double sum = 0;
            Iterator<JSONObject> objIterator = backendConfigList.iterator();
            while (objIterator.hasNext()) {
                JSONObject jsonObject = objIterator.next();
                Double proportion = (Double) jsonObject.get("PROPORTION");
                sum += proportion;
            }

            if(sum != 1) {
                throw new Exception("Error: Invalid backend class proportion");
            }

            backendList = new ArrayList<Backend>(Simulator.NUMBER_OF_BACKENDS);

            objIterator = backendConfigList.iterator();
            while (objIterator.hasNext()) {
                JSONObject jsonObject = objIterator.next();
                Long maxCapacity = (Long) jsonObject.get("CAPACITY");
                Long maxBandWidth = (Long) jsonObject.get("BANDWIDTH") * Backend.DISK_BLOCK_SIZE; // Convert from IOPS to bandwidth
                Double proportion = (Double) jsonObject.get("PROPORTION");
                int numbOfBackendsForClass = Math.round(proportion.floatValue() * totalNumberOfBackEnds);
                while (numbOfBackendsForClass > 0 && backendList.size() < totalNumberOfBackEnds) {
                    Backend backend = new Backend(maxCapacity.intValue(), maxBandWidth.intValue());
                    backendList.add(backend);
                    numbOfBackendsForClass--;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return backendList;
    }

    public void resetBackends() {
	    Iterator<Backend> iterator = this.backendList.iterator();
	    while (iterator.hasNext()) {
	        iterator.next().reset();
        }
    }

	public void resetSimulator() {
        resetTimer();
        resetBackends();

        loadReader = new LoadReader();
    }
}
