package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.UUID;

public class ParDoBQEnrichment  extends DoFn<TableRow,String> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String dividor="{";
        String today_now = ISODateTimeFormat.dateTime().print(new DateTime(org.joda.time.DateTimeZone.UTC));
        TableRow line = c.element();
        //System.out.println("line11="+line);
        TableRow tr = new TableRow();
        tr = line.clone();
        //System.out.println(tr.get("artist_uri"));
        String record = "";
        if (tr.get("artist_image") == null) {
            ReadHTTP rhp = new ReadHTTP();
            String track_uri = line.get("track_uri").toString().split(":")[2];
            String artist_name = line.get("artist_name").toString().replace("{",""); // "{" is used as a dividor, "{" never occur in artist name in our db so far, let it never occur ever

            artist_name = artist_name.replace("\"","'");

            //if(artist_name.contains("\""))
            //    artist_name = artist_name.replace("”","\"");

            rhp.getImages(track_uri,artist_name);
            tr.set("artist_uri",rhp.artist_uri);
            tr.set("artist_image", rhp.artist_image);
            tr.set("track_image", rhp.track_image);
            tr.set("artist_name",artist_name);
            //canopus_id,isrc,track_uri,artist_uri,artist_image,track_image,last_update
            record = tr.get("canopus_id") + dividor + tr.get("artist_name") + dividor + tr.get("isrc") + dividor + tr.get("track_uri") + dividor + tr.get("artist_uri") + dividor + tr.get("track_image") + dividor + tr.get("artist_image") + dividor + today_now;
        }
        System.out.println("Record=" + record);
        c.output(record);
        //c.output(record);
    }
}