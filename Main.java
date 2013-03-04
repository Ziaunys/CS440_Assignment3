import java.io.*;
import java.nio.ByteBuffer;
import java.lang.Long;
import java.lang.String;
import java.lang.Integer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;


class InsertionThread extends Thread {

    Dbs dbs;
    int startIdx;
    int endIdx;
    public InsertionThread() {}
    public ArrayList<File> paths;
    public XMLFileBinding binding;

    public InsertionThread(Dbs dbs1, XMLFileBinding binding1, ArrayList<File> paths1,  int start, int end) {
        this.dbs = dbs1;
        this.binding = binding1;
        this.startIdx = start;
        this.endIdx = end;
        this.paths = paths1;
        System.out.println("Start: " + start + " End: " + end);
    }

    public void run() {
        int ikey = 0;
        File path;
        XMLFile xml;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try {
            for(int i = this.startIdx; i <= this.endIdx; i++) {
                path = this.paths.get(i);
                xml = new XMLFile(path);
				ikey = Integer.parseInt(xml.getName().replaceAll(".xml", ""));
				IntegerBinding.intToEntry(ikey, key);
                binding.objectToEntry(xml, data);
                this.dbs.getDb().put(null, key, data);
            }
        } catch (DatabaseException e) {
            System.err.println("Caught DatabaseException during creation: ");
            e.printStackTrace();
        } catch (NullPointerException npe) {
            System.err.println("Null pointer exception during insertion");
            npe.printStackTrace();
        }
    }
}


public class Main {
    public static String dbName = "imdb";
    public static XMLFileBinding binding = new XMLFileBinding();
    public static Dbs dbs = new Dbs();
	public static OperationStatus ret; 
    public static XMLFile xml;
    public static Cursor cursor;
    public static SecondaryCursor secCursor;
    public static ArrayList<File> paths;
    public static ArrayList<InsertionThread> pool;

    public static void populateDB(String location, int totalThreads) {
        File rootPath = new File(location);
        paths = FileData.walkPath(rootPath);
        try {
            dbs.setup(dbName);
        } catch (DatabaseException e){
            System.err.println("Caught Exception creating datatbase :");
            e.printStackTrace();
        }
        int pathCount = paths.size();
        int interval = pathCount / totalThreads;
        InsertionThread[] threadPool = new InsertionThread[totalThreads];
        try {
            for(int i = 0; i < totalThreads - 1; i ++) {
                threadPool[i] = new InsertionThread(dbs, binding, paths, i * interval, (i + 1) * interval - 1);
            }
            threadPool[totalThreads - 1] = new InsertionThread(dbs, binding, paths, (totalThreads - 1) * interval, pathCount - 1);
            for(InsertionThread thread:threadPool){
                thread.start();
            }
            for(InsertionThread thread:threadPool){
                thread.join();
            }
        /*} catch (NullPointerException npe) {
            System.err.println("Null pointer exception during insertion");
            npe.printStackTrace(); */
        } catch(InterruptedException e) {
            System.err.println("Threads interrupted");
        }
        dbs.close();
    }


    public static XMLFile imdbPointQuery(int fileSize) {
        try {
            dbs.setup(dbName);
        } catch (DatabaseException e){
           System.err.println("Caught Exception creating datatbase :");
           e.printStackTrace();
        }
        XMLFileBinding binding = new XMLFileBinding();
        try {
            secCursor = dbs.getSecDb().openSecondaryCursor(null, null);
            XMLFile foundEntry = null;
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();
            DatabaseEntry sizeKey = new DatabaseEntry();
			IntegerBinding.intToEntry(fileSize, sizeKey);
            ret =  secCursor.getSearchKey(sizeKey, foundData, LockMode.DEFAULT);
			if(ret == OperationStatus.SUCCESS) {
            	xml = (XMLFile) binding.entryToObject(foundData);
			} else {
				xml = null;
			}
        } catch (DatabaseException e) {
            System.err.println("Database Error: " + e.toString());
            e.printStackTrace();
        } catch (NullPointerException npe) {
			System.err.println("Record not found.");
			System.exit(1);
		} finally {
    		dbs.close();
        }
        return xml;
    }


    public static ArrayList<XMLFile> imdbPointQueryText(String ... text) {
        try {
            dbs.setup(dbName);
        } catch (DatabaseException e){
           System.err.println("Caught Exception creating datatbase :");
           e.printStackTrace();
        }
		ArrayList<XMLFile> foundEntries = new ArrayList<XMLFile>();
        Set<String> uniq = null;
        Set<String> searchTerms = new HashSet<String>(FileData.uniqTerms(text));
        ArrayList<String> searchArr = new ArrayList<String>(searchTerms);
        XMLFileBinding binding = new XMLFileBinding();
        try {
            secCursor = dbs.getTextDb().openSecondaryCursor(null, null);
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();
            DatabaseEntry textKey = new DatabaseEntry(searchArr.get(0).getBytes());
            ret = secCursor.getSearchKey(textKey, foundKey, foundData, LockMode.DEFAULT);
            DatabaseEntry correctKey = null;
            Set<DatabaseEntry> keysSeen = new HashSet<DatabaseEntry>();

            while (ret == OperationStatus.SUCCESS) {
                keysSeen.add(foundKey);
                xml = (XMLFile) binding.entryToObject(foundData);
                uniq = FileData.uniqTerms(xml.getContent());
                if(uniq.containsAll(searchTerms)) {
                    foundEntries.add(xml);
                }
                ret = secCursor.getNext(textKey, foundKey, foundData, LockMode.DEFAULT);
                if (keysSeen.contains(foundKey) || !uniq.contains(searchArr.get(0))) {
                    break;
                }
            }
        } catch (DatabaseException e) {
            System.err.println("Database Error: " + e.toString());
            e.printStackTrace();
        } catch (NullPointerException npe) {
			System.err.println("Record not found.");
			System.exit(1);
		} finally {
    		dbs.close();
        }
        return foundEntries;
    }

    public static XMLFile imdbPointQuery(String fileName) {
        XMLFile foundEntry = null;
        try {
            dbs.setup(dbName);
        } catch (DatabaseException e) {
            System.err.println("Caught Exception creating datatbase :");
            e.printStackTrace();
        }
        XMLFileBinding binding = new XMLFileBinding();
        try {
            DatabaseEntry foundData = new DatabaseEntry();
			int ikey = Integer.parseInt(fileName.replaceAll(".xml", ""));
            DatabaseEntry nameKey = new DatabaseEntry();
			IntegerBinding.intToEntry(ikey, nameKey);
            dbs.getDb().get(null, nameKey, foundData, null);
			foundEntry =  (XMLFile) binding.entryToObject(foundData);
        } catch (DatabaseException e) {
            System.err.println("Database Error: " + e.toString());
            e.printStackTrace();
        } catch (NullPointerException npe) {
			System.err.println("Record not found.");
			System.exit(1);
		} finally {
		    dbs.close();
        }
        return foundEntry;
    }


    public static ArrayList<XMLFile> imdbRangeQuery(String fileNameMin, String fileNameMax) {
		ArrayList<XMLFile> foundEntries = new ArrayList<XMLFile>();
		int max = Integer.parseInt(fileNameMax.replaceAll(".xml", ""));
        int current = 0;
		int min = Integer.parseInt(fileNameMin.replaceAll(".xml", ""));
        DatabaseEntry foundData = new DatabaseEntry();
        DatabaseEntry nameKey = new DatabaseEntry();
		IntegerBinding.intToEntry(min, nameKey);
        try {
			dbs.setup(dbName);
		} catch (DatabaseException e) {
			System.err.println("Caught Exception creating database :");
			e.printStackTrace();
	    }
		XMLFileBinding binding = new XMLFileBinding();
        try {
            cursor = dbs.getDb().openCursor(null, null);
	    	ret = cursor.getSearchKeyRange(nameKey, foundData, LockMode.DEFAULT);
	    	xml = (XMLFile) binding.entryToObject(foundData);
            current = Integer.parseInt(xml.getName().replaceAll(".xml", ""));
    		while (ret == OperationStatus.SUCCESS && current <= max) {
                if(current >= min) {
                    foundEntries.add(xml);
                }
                ret = cursor.getNext(nameKey, foundData, LockMode.DEFAULT);
                xml = (XMLFile) binding.entryToObject(foundData);
                current = Integer.parseInt(xml.getName().replaceAll(".xml", ""));
            }
	    } catch (DatabaseException e) {
            System.err.println("Caught Database Exception:");
            e.printStackTrace();
        } catch (NullPointerException npe) {
            System.err.println("Caught Null Pointer Exception:");
            npe.printStackTrace();
        } finally {
            dbs.close();
        }
        return foundEntries;
    }


    public static ArrayList<XMLFile> imdbRangeQuery(int fileSizeMin, int fileSizeMax) {
        ArrayList<XMLFile> foundEntries = new ArrayList<XMLFile>();
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        DatabaseEntry sizeKey = new DatabaseEntry();
		IntegerBinding.intToEntry(fileSizeMin, sizeKey);
        try {
            dbs.setup(dbName);
       } catch (DatabaseException e){
           System.err.println("Caught Exception creating datatbase :");
           e.printStackTrace();
       }
        XMLFileBinding binding = new XMLFileBinding();
        SecondaryCursor secCursor = null;
        try {
            secCursor = dbs.getSecDb().openSecondaryCursor(null, null);
            ret = secCursor.getSearchKeyRange(sizeKey, foundKey, foundData, LockMode.DEFAULT);
            xml = (XMLFile) binding.entryToObject(foundData);
            while (ret == OperationStatus.SUCCESS && xml.getSize() <= fileSizeMax) {
                if(xml.getSize() >= fileSizeMin) {
                    foundEntries.add(xml);
                }
                ret = secCursor.getNext(sizeKey, foundKey, foundData, LockMode.DEFAULT);
                xml = (XMLFile) binding.entryToObject(foundData);
            }
        } catch (DatabaseException e) {
            System.err.println("Database Error: " + e.toString());
            e.printStackTrace();
        } catch (NullPointerException npe) {
            System.err.println("Record not found.");
            System.exit(1);
        } finally {
            dbs.close();
        }
        return foundEntries;
    }


    public static ArrayList<XMLFile> imdbRangeQuery(String fileNameMin, String fileNameMax, int fileSizeMin, int fileSizeMax) {
        ArrayList<XMLFile> found = imdbRangeQuery(fileNameMin, fileNameMax);
        ArrayList<XMLFile> validEntries = new ArrayList<XMLFile>();
        for(XMLFile xml:found) {
            if(fileSizeMin <= xml.getSize() && xml.getSize() <= fileSizeMax) {
                validEntries.add(xml);
            }
        }
        return validEntries;
    }


    public static void main(String[] args) {

        if(args.length < 1) {
			System.out.println("Insufficient args. . ERROR!");
			System.exit(1);
		}
        ArrayList<XMLFile> results =  null;
        XMLFile ret = null;

		switch(args[0].charAt(0)) {
			case 'i':
				System.out.println("Populating database.");
				populateDB(args[1], 8);
				break;
			case 'q':
				System.out.println("Performing query.");
                dbName = args[1];
				try {
                    File outFile = new File("Eric_Zounese_Assignment3");
                    if (!outFile.exists()) {
                        outFile.createNewFile();
                    }
					String line;
					BufferedReader br = new BufferedReader(new FileReader(args[2]));
                    FileWriter fw = new FileWriter(outFile.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
					while((line = br.readLine()) != null) {
                        bw.write("Query with the following terms ");
                        for(String token:line.split(" ")){
                            bw.write(token + " ");
                        }
                        bw.write("\n");
                        results = imdbPointQueryText(line.split(" "));
                        for(XMLFile result:results) {
                            bw.write(result.getName() + ";");
                            System.out.println(result.getName());
                        }
                        bw.write("\n");
                    }
                    bw.close();
                } catch(IOException ioe) {
                    System.out.println("Caught file exception.");
                }
        }
    }
}
