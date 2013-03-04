import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.SecondaryDatabase;
import com.sleepycat.db.SecondaryConfig;

import java.util.Comparator;
import java.io.FileNotFoundException;

public class Dbs {

    private Database imdb = null;
    private String imdbName = "primary";
    private String secName = "secondary";
	private String textName = "text";
    private SecondaryDatabase sizeDb = null;
	private SecondaryDatabase textDb = null;
    private XMLFileBinding dbBind = null;
    private SizeKeyCreator secKey = null;
	private TextIndexKeyCreator textKey = null;
	private IndexComparator indexCmp = new IndexComparator();
    public Dbs() {}

    public void setup(String dbNames) throws DatabaseException {
        dbBind = new XMLFileBinding();
        secKey = new SizeKeyCreator(dbBind);
		textKey = new TextIndexKeyCreator(dbBind);

        DatabaseConfig dbConfig = new DatabaseConfig();
        SecondaryConfig secDbConfig = new SecondaryConfig();
		SecondaryConfig textDbConfig = new SecondaryConfig();
        dbConfig.setErrorStream(System.err);
        dbConfig.setErrorPrefix("Databases");
        dbConfig.setType(DatabaseType.BTREE);
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(false);
        dbConfig.setCacheSize(1000000);
		dbConfig.setBtreeComparator(indexCmp);

        secDbConfig.setErrorStream(System.err);
        secDbConfig.setErrorPrefix("Secondary");
        secDbConfig.setKeyCreator(secKey);
        secDbConfig.setType(DatabaseType.BTREE);
        secDbConfig.setSortedDuplicates(true);
        secDbConfig.setAllowPopulate(true); 
        secDbConfig.setAllowCreate(true);
        secDbConfig.setTransactional(false);
        secDbConfig.setCacheSize(1000000);
		secDbConfig.setBtreeComparator(indexCmp);

        textDbConfig.setErrorStream(System.err);
        textDbConfig.setErrorPrefix("textdb");
        textDbConfig.setMultiKeyCreator(textKey);
        textDbConfig.setType(DatabaseType.BTREE);
        textDbConfig.setSortedDuplicates(true);
        textDbConfig.setAllowPopulate(true); 
        textDbConfig.setAllowCreate(true);
        textDbConfig.setTransactional(false);
        textDbConfig.setCacheSize(1000000);


        try {
            System.out.println("Database at: " + imdbName);
            imdb = new Database(dbNames + "/" + imdbName, null, dbConfig);
        } catch(FileNotFoundException notFound) {
            System.err.println(" HI Databases: " + notFound.toString());
            notFound.printStackTrace();
            System.exit(-1);
        }

        try {
            sizeDb = new SecondaryDatabase(dbNames + "/" + secName, secName, imdb, secDbConfig);
        } catch(FileNotFoundException e) {
            System.err.println(" Error in Secondary creation : " + e.toString());
            e.printStackTrace();
        }

		try {
			textDb = new SecondaryDatabase(dbNames + "/" + textName, textName, imdb, textDbConfig);
		} catch(FileNotFoundException e) {
			System.err.println("Error in TextDB creation :" + e.toString());
			e.printStackTrace();
		}
    }

    public Database getDb() {
        return imdb;
    }

    public SecondaryDatabase getSecDb() {
        return sizeDb;
    }

    public SecondaryDatabase getTextDb() {
        return textDb;
    }
    public void close() {

        try {
            if (textDb != null) {
                textDb.close();
            }
			if (sizeDb != null) { 
				sizeDb.close();
			}	
            if (imdb != null) {
                imdb.close();
            }

        } catch(DatabaseException dbe) {
            System.err.println("Error closing Databases: " + dbe.toString());
            System.exit(-1);
        }
    }
}
