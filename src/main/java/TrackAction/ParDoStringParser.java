package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import java.util.ArrayList;

public class ParDoStringParser extends DoFn<String,TableRow> {
    ArrayList<String> schemafields = null;
    String dividor="\",\"";

    public ParDoStringParser(ArrayList<String> arg){
        schemafields = arg;
    }
    TableRow getTableRow(String line) {
        line=line.substring(1,line.length()-1);
        String[] ss = line.split(dividor);
        //System.out.println(line);
        //line=line.replace("Du söker bråk, jag kräver dans","Du söker bråk; jag kräver dans");

        TableRow tr = new TableRow();
        for(int i=0; i<schemafields.size();i++) {
            String[] ff = schemafields.get(i).split(":");
            //System.out.println(schemafields.get(i));
            //System.out.println(ff[0]+" = "+ss[i]);
            if(ff.equals("INTEGER")) {
                tr.set(ff[0], Integer.parseInt(ss[i]));
            } else {
                if(ff[1].equals("TIMESTAMP") && ss[i].equals("None"))
                    tr.set(ff[0], "2000-01-01 00:00");
                else if(ff[1].equals("INTEGER") && ss[i].equals("None"))
                    tr.set(ff[0], "0");
                else
                    tr.set(ff[0], ss[i]);
            }
        }
        return tr;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String line = c.element();
        //System.out.println("l="+line);
        if(line.trim().length()>0) {
            TableRow tr = getTableRow(line);
            c.output(tr);
        }
    }
}