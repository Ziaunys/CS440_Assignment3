import java.io.*;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class XMLFileBinding extends TupleBinding {

    public void objectToEntry(Object object, TupleOutput to) {
        XMLFile xmlfile = (XMLFile) object;
        to.writeString(xmlfile.name);
        to.writeInt(xmlfile.size);
        to.writeString(xmlfile.content);
    }

    public Object entryToObject(TupleInput ti) {
        String name = ti.readString();
        int size = ti.readInt();
        String content = ti.readString();
        XMLFile xmlfile = new XMLFile();
        xmlfile.name = name;
        xmlfile.size = size;
        xmlfile.content = content;
        return xmlfile;
    }
}
