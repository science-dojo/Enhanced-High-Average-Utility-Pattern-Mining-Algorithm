package ehaupm;


public class CAUEntry {
	
	// transaction id
	final int tid ;   
	// itemset utility
	final int utility;
	// remaining maximal utility
	int remu;
	// revised maximal utility
	int rmu;
	

	public CAUEntry(int tid, int utility, int rmu, int remu){
		this.tid = tid;
		this.utility = utility;
		this.remu = remu;
		this.rmu = rmu;
	}
}
