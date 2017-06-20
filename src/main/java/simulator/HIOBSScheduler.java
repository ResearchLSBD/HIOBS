package simulator;

import java.util.*;


public class HIOBSScheduler extends Scheduler {

    public HIOBSScheduler() {
        super(ScheduleMethod.HIOBS);
        requestQueue = new LinkedList<VolumeRequest>() ;
        candidateList = new ArrayList<Backend>();
        numOfNoCandidate = 0;
    }

    @Override
    public void schedule(ArrayList<Backend> backendList) {

        Iterator<VolumeRequest> itVr = requestQueue.iterator();
        while (itVr.hasNext()) {
            VolumeRequest vr = (VolumeRequest) itVr.next();
            filtering(backendList, vr.getSize());

            if (candidateList.size() == 0) {
                numOfNoCandidate++;
                logger.writeToEventLog("SCHEDULE FAIL: NO CANDIDATE !!! 	" + numOfNoCandidate + "	" + vr.toString());
            } else {
                Backend backend = weighting(vr);
                logger.writeToEventLog("SCHEDULE SUCCESS VR: " + vr.toString() + " to " + backend.getId());
                Volume v = backend.createVolume(vr);
                backend.readVolume(v);
                itVr.remove();
                return;
            }
        }

    }

    private void filtering(ArrayList<Backend> backendList, int size) {

        candidateList.clear();
        String bId = " ";
        for (Backend backend : backendList) {
            if (backend.getAllocatedCapacity() + size <= backend.MAX_CAPACITY) {
                candidateList.add(backend);
                bId += Integer.toString(backend.getId()) + " ";
            }
        }

        logger.writeToEventLog("FILTER DONE list: " + bId);
    }

    private static Backend weighting(final VolumeRequest vr) {
        Collections.sort(candidateList, new Comparator<Backend>() {
            public int compare(Backend o1, Backend o2) {
                int av1 = o1.getAvailableVolumeSpeed() - vr.getSLA();
                int av2 = o2.getAvailableVolumeSpeed() - vr.getSLA();

                if(av1 < 0){
                    av1 = 999999999;
                }

                if(av2 < 0){
                    av2 = 999999999;
                }

                if(av1 < av2) {
                    return -1;
                }else if(av1 == av2) {
                    return 0;
                }else{
                    return 1;
                }
            }
        });

        if (Simulator.DEBUG) {
            String bId = "availVolumeSpeed ";
            for (Backend backend : candidateList) {
                bId += Integer.toString(backend.getId()) + " " + (backend.getAvailableVolumeSpeed() + " ");
            }

            Logger logger = Logger.getLoggerInstance();
            logger.writeToEventLog("WEIGHTING DONE list: " + bId);
        }

        return candidateList.get(0);
    }

    @Override
    public void addNewRequestToQ(VolumeRequest vr) {
        if (requestQueue.size() >= MAX_Q_LENGTH) {
            logger.writeToEventLog("INSERT Q FAIL: VR: " + vr.getId() + " Q SIZE:" + requestQueue.size());
            logger.writeToFailCapLog(Integer.toString(vr.getSize()/1000));
            //failCap += vr.getSize();
            return;
        } else {
            requestQueue.add(vr);
            logger.writeToEventLog("INSERT Q VR: " + vr.toString() );
        }

        //logger.writeToEventLog("INSERT Q VR: " + vr.toString() + " " + this.qToString());
    }

    @Override
    public boolean isQEmpty() {
        return requestQueue.isEmpty();
    }

}
