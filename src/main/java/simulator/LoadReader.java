package simulator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LoadReader {
	
	BufferedReader br;
	String inputName;

	public LoadReader() {
		
		if (Simulator.DEBUG) {
			inputName = new String("test.txt");
		} else {
			inputName = new String("input.txt");
		}
		
		try{
			br = new BufferedReader(new FileReader(inputName));
		} catch (IOException e) {
			e.printStackTrace();	
		}
	}
	
	public VolumeRequest readOneRequest() {

		String[] nums = null;
		
		try {
			String str = br.readLine();
			if (str == null) 
				return null;
			nums = str.split(" ");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return new VolumeRequest(Integer.parseInt(nums[0]),		// id
								Integer.parseInt(nums[1]),		//arrival time
								Integer.parseInt(nums[3]), 		//size
								Integer.parseInt(nums[2]), 		//duration
								Integer.parseInt(nums[4]));		//sla
	}
	
}
