package simulator;

import java.util.*;


public class ThrScheduler extends Scheduler {

    public ThrScheduler() {
        super(ScheduleMethod.THR);
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
                Backend backend = weighting();
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

    private static Backend weighting() {
        Collections.sort(candidateList, new Comparator<Backend>() {

            //@Override
            public int compare(Backend o1, Backend o2) {
                return (o2.getAvailableVolumeSpeed()) - (o1.getAvailableVolumeSpeed());
            }
        });

        if (Simulator.DEBUG) {
            String bId = "availVolumeSpeed ";
            for (Backend backend : candidateList) {
                bId += Integer.toString(backend.getId()) + " " + (backend.getAvailableVolumeSpeed() + " ");
            }

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