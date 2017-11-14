package ehaupm;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;



public class EHAUPM {
	
	// The time at which the algorithm started
	public long startTimestamp = 0;  
	
	// The time at which the algorithm ended
	public long endTimestamp = 0; 
	
	// The number of high average utility itemset
	public int nhauis =0;
	
	// The number of candidate high-utility itemsets
	public long joinCount =0;
	
	// Map to remember the AUUB of each item
	Map<Integer, Long> items2auub;

	
	// The eaucs structure
	Map<Integer, Map<Integer, Integer>> EAUCM;

	
	// Buffer for itemsets, initial buffer size is 200.
	int BUFFERS_SIZE = 200;
	private int[] itemsetBuffer = null;

	// Write HAUIs in a specified file
    BufferedWriter writer = null;

	// Memory recoder: an thread is used to dynamically record peak-memory
    // And the peak-memory is used as the final memory usage.
	MemoryUpdateRunnable memRecorder = new MemoryUpdateRunnable();

	// This class represent an item and its utility in a transaction
	class Pair{
		int item = 0;
		int utility = 0;
	}
	

	public EHAUPM() {
		
	}

	
	
	private Map<Integer, Integer> readProfits(String profitPath) throws Exception {
		Map<Integer, Integer> item2profits = new HashMap<>();
		BufferedReader in = new BufferedReader(new FileReader(profitPath));
		String line = null;
		String[] pair = null;
		while ( (line = in.readLine())!=null) {
			pair = line.split(", ");
			item2profits.put(Integer.parseInt(pair[0].trim()), Integer.parseInt(pair[1].trim()));
		}
		in.close();
		return item2profits;
	}
	

	
	/**
	 * Run the algorithm
     * @param HAUIsFile specify the file which is used to store the discovered HAUIs
     * @param profitPath specify local address of the file records the profit of each item.
     * @param quantityDBPath specify local address of the file records the quantities of each item.
     * @param delta minimum high average utility threshold.
	 * @throws Exception
	 */
	public void runAlgorithm(String HAUIsFile, String profitPath, String quantityDBPath, double delta) throws Exception {

	    // Reset
		Thread timeThread = new Thread(memRecorder);
		memRecorder.isTestMem = true;
		timeThread.start();
		
		// Initialize the buffer for storing the current itemset
		itemsetBuffer = new int[BUFFERS_SIZE];

		// Initialize the structure of EAUCM, which is used to store the auub value of all 2-itemsets whose auub >= delta*total_utility
		EAUCM =  new HashMap<Integer, Map<Integer, Integer>>();

		// Initialize writer
		if(HAUIsFile!=null && !HAUIsFile.equalsIgnoreCase("null"))
        	writer = new BufferedWriter(new FileWriter(HAUIsFile));
		

		// Create a map to store the auub value of each item
		items2auub = new HashMap<Integer, Long>();

        // Read profit of each item and store them
		Map<Integer, Integer> item2profits = readProfits(profitPath);
		
		// At first, scan the database to calculate the auub value of each item.
		BufferedReader dbReader = null;
		String curTran; // current transaction
        // TU: total utility of database; minUtility: minimum high average utility
		long TU=0, minUtility=0;
		try {
			dbReader = new BufferedReader(new InputStreamReader( new FileInputStream(new File(quantityDBPath))));
			// for each transaction until the end of file
			int quantity, itemName;
			while ((curTran = dbReader.readLine()) != null) {

				// split the transaction [items in transaction is seperated by ", "]
				String items[] = curTran.split(", ");

				// add utility of each item to TU
                // find the maximal utility of each transaction
				int maxItemUtility = 0;
				for(int i=0; i <items.length; i+=2){
					quantity = Integer.parseInt(items[i].trim());
					itemName = Integer.parseInt(items[i+1].trim());
					int itemUtils = quantity * item2profits.get(itemName);
					TU += itemUtils;
					if(maxItemUtility < itemUtils){
						maxItemUtility = itemUtils;
					}
				}

				//
				for(int i=0; i <items.length; i+=2){
					itemName = Integer.parseInt(items[i+1].trim());
					Long auub = items2auub.get(itemName);
					auub = (auub == null) ? maxItemUtility : auub + maxItemUtility;
					items2auub.put(itemName, auub);
				}
			}
			minUtility =(long)(TU*delta);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(dbReader != null){
				dbReader.close();
			}
	    }
		
		// Container of CAUList of each item whose auub value >= minUtility.
		List<CAUList> listOfCAULists = new ArrayList<CAUList>();
		// Key: item, Value:CAUList
		Map<Integer, CAUList> mapItemToUtilityList = new HashMap<Integer, CAUList>();
		

		for(Entry<Integer,Long> entry: items2auub.entrySet()) {
			Integer item = entry.getKey();
			if(items2auub.get(item) >= minUtility) {
				CAUList uList = new CAUList(item);
				mapItemToUtilityList.put(item, uList);
				listOfCAULists.add(uList);
			}
		}
		// Sort CAUList according to its auub-ascending order
		Collections.sort(listOfCAULists, new Comparator<CAUList>(){
			public int compare(CAUList o1, CAUList o2) {
				return compareItems(o1.item, o2.item);
			}
		} );
		
		// Scan DB again to construct CAUList of each item whose auub value >= minUtility.
		try {
			dbReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(quantityDBPath))));

			int tid =0;
			int quantity,itemName;
			while ((curTran = dbReader.readLine()) != null) {
				String items[] = curTran.split(", ");
				List<Pair> revisedTransaction = new ArrayList<Pair>();

				int maxUtilityOfCurTrans =0;
				for(int i=0; i <items.length; i+=2) {
					quantity = Integer.parseInt(items[i].trim());
					itemName = Integer.parseInt(items[i+1].trim());
					int tmputility = quantity * item2profits.get(itemName);
					
					Pair pair = new Pair();
					pair.item = itemName;
					pair.utility = tmputility;
					
					// if the item has enough utility
					if(items2auub.get(pair.item) >= minUtility) {

						if(maxUtilityOfCurTrans < pair.utility){
							maxUtilityOfCurTrans = pair.utility;
						}
						revisedTransaction.add(pair);
					}
				}

				// sort the transaction according to auub-ascending order
				Collections.sort(revisedTransaction, new Comparator<Pair>(){
					public int compare(Pair o1, Pair o2) {
					    return compareItems(o1.item, o2.item);
					}
				});

                // Get remu value and rmu value without extra computation
				int remu=0, rmu=0;
				for(int i = revisedTransaction.size()-1; i>=0; --i){

					Pair pair =  revisedTransaction.get(i);

					rmu = rmu < pair.utility ? pair.utility : rmu;

					// get the utility list of this item
					CAUList CAUListOfItem = mapItemToUtilityList.get(pair.item);

					// Add a new CAUEntry to the CAUList of this item corresponding to this transaction
					CAUEntry CAUEntry = new CAUEntry(tid, pair.utility, rmu, remu);
					CAUListOfItem.addElement(CAUEntry);

					remu = (remu<pair.utility) ? pair.utility : remu;
				}

				// Construct EAUCM structure for store auub value of 2-itemset
				for(int i = 0; i< revisedTransaction.size(); i++){
					Pair pair =  revisedTransaction.get(i);
					Map<Integer, Integer> subEAUCS = EAUCM.get(pair.item);
					if(subEAUCS == null) {
						subEAUCS = new HashMap<Integer, Integer>();
						EAUCM.put(pair.item, subEAUCS);
					}
					for(int j = i+1; j< revisedTransaction.size(); j++){
						Pair pairAfter = revisedTransaction.get(j);
						Integer auubSum = subEAUCS.get(pairAfter.item);
						if(auubSum == null) {
							subEAUCS.put(pairAfter.item, maxUtilityOfCurTrans);
						}else {
							subEAUCS.put(pairAfter.item, auubSum + maxUtilityOfCurTrans);
						}
					}

				}
				++tid;

			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(dbReader != null){
				dbReader.close();
			}
	    }

		// Enumerate the enumeration-tree
		search(itemsetBuffer, 0, null, listOfCAULists, minUtility);
		
		memRecorder.isTestMem =false;
		if(writer!=null)
            writer.close();

	}

	private int compareItems(int item1, int item2) {
		int compare = (int)( items2auub.get(item1) - items2auub.get(item2));
		// if the same, use the lexical order otherwise use the auub value
		return (compare == 0)? item1 - item2 :  compare;
	}
	
	/**
	 * @param prefix  This is the current prefix. Initially, it is empty.
	 * @param ULOfPxy This is the Utility List of the prefix. Initially, it is empty.
	 * @param ULs The utility lists corresponding to each extension of the prefix.
	 * @param minUtility The minUtility threshold.
	 * @param prefixLength The current prefix length
	 * @throws IOException
	 */
	private void search(int [] prefix,
                        int prefixLength, CAUList ULOfPxy, List<CAUList> ULs, long minUtility)
			throws IOException {
		

		for(int i=0; i< ULs.size(); i++) {
			CAUList X = ULs.get(i);

			if( (X.sumutils / (double)(prefixLength+1)) >= minUtility) {
                nhauis++;
                if(writer!=null)
				    writeOut(prefix, prefixLength, X.item, X.sumutils / (double)(prefixLength+1));
			}

			// Proposed loose upper bound
			if((X.sumutils / (double)(prefixLength+1) + X.sumOfRemu) < minUtility){
				continue;
			}
			
			// Proposed revised uppper bound
			if(X.sumOfRmu >= minUtility){
				// This list will contain the CAUList of Px 1-extensions.
				List<CAUList> extensionOfPx = new ArrayList<>();
				// For each extension of P appearing
				// after x according to the ascending order
				for(int j=i+1; j < ULs.size(); j++){

				    CAUList Y = ULs.get(j);
					Map<Integer, Integer> auub1 = EAUCM.get(X.item);

					if(auub1 != null) {
						Integer auub2 = auub1.get(Y.item);
						if(auub2 == null || auub2 < minUtility) {
							continue;
						}
					}
					joinCount++;

					// we construct CAUList of Pxy and add it to the list of extensions of pX
					// For some datasets, `construct_opt` and `construct` have different running status.
					// Not all of them are always better than the other
					CAUList pxy = construct_opt(prefixLength+1, ULOfPxy, X, Y, minUtility);

					if(pxy != null) {
						extensionOfPx.add(pxy);
					}
				}
				// Create new prefix Px
                if(prefixLength==BUFFERS_SIZE) {
				    BUFFERS_SIZE = BUFFERS_SIZE + (int)(BUFFERS_SIZE/2);
                    int[] tmp = new int[BUFFERS_SIZE];
                    System.arraycopy(tmp,0, itemsetBuffer, 0, prefixLength);
                    itemsetBuffer = tmp;
                }
				itemsetBuffer[prefixLength] = X.item;
				// Recursive call to discover all itemsets with the prefix Px
				search(itemsetBuffer, prefixLength+1, X, extensionOfPx, minUtility);
			}
		}
	}
	
	/**
	 * @param P :  the utility list of prefix P.
	 * @param px : the utility list of pX
	 * @param py : the utility list of pY
	 * @return the CAU list of pXY
	 */
	private CAUList construct(int prefixLen, CAUList P, CAUList px, CAUList py, long minUtility) {
		// create an empy utility list for Pxy
		CAUList pxyUL = new CAUList(py.item);

		long sumOfRmu = px.sumOfRmu;
		long sumOfRemu = (long)(px.sumutils / (double)prefixLen + px.sumOfRemu);

		// For each element in the utility list of pX
		for(CAUEntry ex : px.CAUEntries) {
			// Do a binary search to find element ey in py with tid = ex.tid
			CAUEntry ey = findElementWithTID(py, ex.tid);
			if(ey == null) {
                sumOfRmu -= ex.rmu;
                sumOfRemu -= (ex.utility /(double)prefixLen + ex.remu);
                if(Math.min(sumOfRemu, sumOfRmu) < minUtility) {
                    return null;
                }
				continue;
			}

			// If the prefix p is null
			if(P == null){
				// Create the new element
				CAUEntry eXY = new CAUEntry(ex.tid, ex.utility + ey.utility, ex.rmu, ey.remu);

				// add the new element to the utility list of pXY
				pxyUL.addElement(eXY);

			} else {
				// find the element in the utility list of p wih the same tid
				CAUEntry e = findElementWithTID(P, ex.tid);
				if(e != null){
					// Create new element
					CAUEntry eXY = new CAUEntry(ex.tid, ex.utility + ey.utility - e.utility,
							ex.rmu, ey.remu);
					// add the new element to the utility list of pXY
					pxyUL.addElement(eXY);
				}
			}	
		}
		// return the utility list of pXY.
		return pxyUL;
	}
	
	private CAUList construct_opt(int prefixLen,CAUList P, CAUList Px, CAUList Py, long minUtility) {
		// create an empy utility list for pXY
		CAUList pxyUL = new CAUList(Py.item);
		long sumOfRmu = Px.sumOfRmu;
		long sumOfRemu =(long)(Px.sumutils /(double)prefixLen + Px.sumOfRemu) ;
		int idxPx=0, idxPy=0;

		while(idxPx < Px.CAUEntries.size() && idxPy < Py.CAUEntries.size()) {
            CAUEntry ex = Px.CAUEntries.get(idxPx);
            CAUEntry ey = Py.CAUEntries.get(idxPy);

            if(ex.tid==ey.tid) {
                if(P!=null) {
                    CAUEntry e = findElementWithTID(P, ex.tid);
                    if(e!=null) {
                        // Create the new element
                        CAUEntry eXY = new CAUEntry(ex.tid, ex.utility + ey.utility-e.utility, ex.rmu, ey.remu);
                        // add the new element to the utility list of pXY
                        pxyUL.addElement(eXY);
                    }
                } else {
                    // Create the new element
                    CAUEntry eXY = new CAUEntry(ex.tid, ex.utility + ey.utility, ex.rmu, ey.remu);
                    // add the new element to the utility list of pXY
                    pxyUL.addElement(eXY);
                }

                ++idxPx; ++idxPy;
            } else if (ex.tid > ey.tid) {
                ++idxPy;
            }
            else { // ex.tid < ey.tid : not find entry whose tid == ex.tid in CAU-List of py
                ++idxPx;
                sumOfRmu -= ex.rmu;
                sumOfRemu -= (ex.utility/(double)prefixLen + ex.remu);
                if(Math.min(sumOfRmu,sumOfRemu) < minUtility) { // select the bigger between them
					return null;
                }
            }
        }

		return pxyUL;
	}
	
	/**
	 * Do a binary search to find the element with a given tid in a utility list
	 * @param ulist the utility list
	 * @param tid  the tid
	 * @return  the element or null if none has the tid.
	 */
	private CAUEntry findElementWithTID(CAUList ulist, int tid){
		List<CAUEntry> list = ulist.CAUEntries;
		
		// perform a binary search to check if  the subset appears in  level k-1.
        int first = 0;
        int last = list.size() - 1;
       
        // the binary search
        while( first <= last )
        {
        	int middle = ( first + last ) >>> 1; // divide by 2

            if(list.get(middle).tid < tid){
            	first = middle + 1;  //  the itemset compared is larger than the subset according to the lexical order
            }
            else if(list.get(middle).tid > tid){
            	last = middle - 1; //  the itemset compared is smaller than the subset  is smaller according to the lexical order
            }
            else{
            	return list.get(middle);
            }
        }
		return null;
	}

	/**
	 * @param prefix the prefix to be writent o the output file
	 * @param item an item to be appended to the prefix
	 * @param utility the utility of the prefix concatenated with the item
	 * @param prefixLength the prefix length
	 */
	private void writeOut(int[] prefix, int prefixLength, int item, double utility) throws IOException {

		//Create a string buffer
		StringBuilder buffer = new StringBuilder();
		// append the prefix
		for (int i = 0; i < prefixLength; i++) {
			buffer.append(prefix[i]);
		}
		// append the last item
		buffer.append(item);
		// append the average-utility value
		buffer.append(String.format(":%.2f\n", utility));
		// write to file
		writer.write(buffer.toString());

	}



    
    class MemoryUpdateRunnable implements Runnable {
    	boolean isTestMem;
    	double maxConsumationMemory;
		@Override
		public void run() {
			while (this.isTestMem) {
				double currentMemory = (Runtime.getRuntime().totalMemory() - Runtime
						.getRuntime().freeMemory()) / 1024d / 1024d;
			 
				if(currentMemory > maxConsumationMemory) {
					maxConsumationMemory = currentMemory;
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
				}
			}
		}
	}
}