package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;

public class MapWithKey extends DoFn<TableRow,KV<String,TableRow>> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        TableRow line = c.element();
        //System.out.println(line);
        JSONObject obj = new JSONObject(line);
        String key = obj.getString("email");
        System.out.println("key="+key+" line="+line.toPrettyString());
        c.output(KV.of(key, line));
    }
}
