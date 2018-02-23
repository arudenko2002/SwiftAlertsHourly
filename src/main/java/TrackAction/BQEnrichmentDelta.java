package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.UUID;

public class BQEnrichmentDelta extends DoFn<TableRow,TableRow> {
    private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static String project="umg-dev";
    private static String dataSet="swift_alerts";
    private static String tableName="artist_track_images";

    static void executeQueue(String query,String project, String dataSet, String tableName) throws Exception {
        //System.out.println(query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        .setUseLegacySql(false)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        // Wait for the query to complete.
        queryJob = queryJob.waitFor();
        System.out.println("Query completed");
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.

        QueryResponse response = bigquery.getQueryResults(jobId);

        QueryResult result = response.getResult();
        System.out.println("RESULT:"+result);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String today_now = ISODateTimeFormat.dateTime().print(new DateTime(org.joda.time.DateTimeZone.UTC));
        TableRow line = c.element();
        //System.out.println("line11="+line);
        TableRow tr = new TableRow();
        tr=line.clone();
        //System.out.println(tr.get("artist_uri"));
        String record="";
        if(tr.get("artist_image")==null) {
            ReadHTTP rhp = new ReadHTTP();
            String track_uri = line.get("track_uri").toString().split(":")[2];
            String artist_name = line.get("artist_name").toString();
            rhp.getImages(track_uri,artist_name);
            tr.set("artist_image", rhp.artist_image);
            tr.set("track_image", rhp.track_image);
            String query = "INSERT `"+project+"."+dataSet+"."+tableName+"` (canopus_id,isrc,track_uri,artist_uri,track_image,artist_image,last_update) VALUES "+
                    "("+tr.get("canopus_id")+",\""+tr.get("isrc")+"\",\""+tr.get("track_uri")+"\",\""+tr.get("artist_uri")+"\",\""+tr.get("track_image")+"\",\""+tr.get("artist_image")+"\",\""+today_now+"\")";
            System.out.println("query="+query);
            executeQueue(query,project,dataSet,tableName);
            record=tr.get("track_uri")+","+tr.get("artist_image")+","+tr.get("track_image")+","+today_now;
        }
        //System.out.println("Record="+tr);
        c.output(tr);
        //c.output(record);
    }
}
