import java.io.*;
import java.nio.ByteBuffer;

public class XMLFile {

    public String name;
    public int size;
    public String content;

    public XMLFile () {
    }

    public XMLFile (File f) {
        name = f.getName();
        size = (int) f.length();
        content = FileData.getFileData(f);
    }

    public XMLFile (String name, int size, String content) {
        this.name = name;
        this.size = size;
        this.content = content;
    }

    public String getName() {
        return this.name;
    }
	
	public int getSize() { 
		return this.size;
	}

	public String getContent() {
		return this.content;
	}

    public String toString() {
           return  "<XMLFile: " + this.name + "/>\n " + "< XMLFileSize: " + this.size + "/>\n" + 
			"<XMLContent: " + this.content + "/>\n";
    }
}
