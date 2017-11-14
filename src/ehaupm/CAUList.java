package ehaupm;




import java.util.ArrayList;
import java.util.List;


public class CAUList {
    // the item
	int item;
	// sum of utilities
	long sumutils = 0;
	// sum of remaining utilities
	long sumOfRemu = 0;
    // sum of revised utilities
	long sumOfRmu = 0;
	// container of CAUEntry,
	List<CAUEntry> CAUEntries = new ArrayList<CAUEntry>();

	public CAUList(int item){
		this.item = item;
	}

	public void addElement(CAUEntry CAUEntry) {
		sumutils += CAUEntry.utility;
		sumOfRmu += CAUEntry.rmu;
		sumOfRemu += CAUEntry.remu;
		CAUEntries.add(CAUEntry);
	}

}
