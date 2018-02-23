package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

public class BQEnrichment extends DoFn<TableRow,TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String today_now = ISODateTimeFormat.dateTime().print(new DateTime(org.joda.time.DateTimeZone.UTC));
        ReadHTTP rhp = new ReadHTTP();
        TableRow line = c.element();
        System.out.println("line="+line);
        TableRow tr = new TableRow();
        tr=line.clone();
        String track_uri = line.get("track_uri").toString().split(":")[2];
        rhp.getImages(track_uri,"");
        tr.set("artist_image", rhp.artist_image);
        tr.set("track_image", rhp.track_image);
        tr.set("last_update", today_now);
        System.out.println("line2="+tr);
        c.output(tr);
    }
}

