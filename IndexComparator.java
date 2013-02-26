import java.util.Comparator;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.db.DatabaseEntry;
public class IndexComparator implements Comparator<byte[]> { 

	@Override 	
	public int compare(byte[] d1, byte[] d2) { 
		
		return IntegerBinding.entryToInt(new DatabaseEntry(d1)) - IntegerBinding.entryToInt(new DatabaseEntry(d2));
	}
}	
		
