import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;

import java.io.FileNotFoundException;

public class BaseDatabase {

    private Database imdb = null;
    private String imdb_name = "imdb";

    public BaseDatabase() {}

    public void setup(String dbNames)
        throws DatabaseException {

        DatabaseConfig dbConfig = new DatabaseConfig();

        dbConfig.setErrorStream(System.err);
        dbConfig.setErrorPrefix("Databases");
        dbConfig.setType(DatabaseType.BTREE);
        dbConfig.setAllowCreate(true);

        try {
            imdb_name = dbNames + "/" + imdb_name;
            System.out.println("Database at: " + imdb_name);
            imdb = new Database(imdb_name, null, dbConfig);
        } catch(FileNotFoundException notFound) {
            System.err.println(" HI Databases: " + notFound.toString());
            System.exit(-1);
        }
    }

    public Database getDb() {
        return imdb;
    }

    public void close() {
        try {
            if (imdb != null) {
                imdb.close();
            }

        } catch(DatabaseException dbe) {
            System.err.println("Error closing Databases: " + dbe.toString());
            System.exit(-1);
        }
    }
}
