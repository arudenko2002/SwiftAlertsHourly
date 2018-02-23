package TrackAction;

import com.google.cloud.bigquery.*;

import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;

public class SendTrackAction {
    public String whom="justtome";
    public Boolean to_email=false;
    public Boolean to_json=false;
    public String project="umg-dev";
    public String runner="DirectRunner";
    private String dataSet="swift_alerts";
    private String tableName="playlist_track_action";
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private String fields="firstname,email,artist_name,artist_uri,product_title,track_uri,name,playlist_uri,owner_id,country,followers,position,action_type,artist_image,track_image";
    //private static ReadHTTP rht=new ReadHTTP();

    void setProject(String project) {
        this.project=project;
    }

    void setRunner(String runner) {
        this.runner=runner;
    }

    void executeQueueGeneral(String _dataSet, String _tableName,String executionDate) throws Exception{
        dataSet=_dataSet;
        tableName=_tableName;
        String actualDate=getDaysAgo(executionDate,-1);
        String source=project+"."+dataSet+"."+tableName;
        String query ="SELECT "+fields+" FROM `"+source+"` WHERE _PARTITIONTIME = TIMESTAMP('"+actualDate+"') ORDER BY email,artist_name,product_title;";
        executeQueue(query,dataSet, tableName,actualDate);
    }

    void executeQueue(String queue,String dataSet, String tableName,String actualDate) throws Exception{
        System.out.println(queue);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(queue)
                        .setUseLegacySql(false)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

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
        if(result.getTotalRows()>0) {
            System.out.println("HERE");
            processQuerySaveJson(result, actualDate);
        } else {
            sendMailToMe("No alerts were detected today!","Warning: no alerts were detected today!","justtome");
        }
    }

    void processQuerySaveJson(QueryResult result, String actualDate) throws Exception {
        String f_email = "";
        String f_artist_name = "";
        String f_track_uri = "";
        JSONArray playlists = new JSONArray();
        JSONObject track = new JSONObject();
        JSONArray tracks = new JSONArray();
        JSONObject artist = new JSONObject();
        JSONArray artists = new JSONArray();
        JSONObject user = new JSONObject();
        if(result==null || result.getTotalRows()==0) {
            System.out.println("No emails to send.  Exiting..");
            return;
        }
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {

                HashMap<String,String> record = getRecord(row);
                String o_email=record.get("email");
                String o_artist_name = record.get("artist_name");
                String o_track_uri = record.get("track_uri");

                JSONObject playlist = getPlaylistJson(record);
                if(f_email.length()>0 && f_email.equals(o_email) && f_artist_name.equals(o_artist_name) && f_track_uri.equals(o_track_uri)) {
                    playlists.put(playlist);
                } else if(f_email.length()>0 && f_email.equals(o_email) && f_artist_name.equals(o_artist_name) && !f_track_uri.equals(o_track_uri)) {
                    track.put("playlists",playlists);
                    tracks.put(track);

                    //Initialize, strt new track
                    track = getTrackJson(record);
                    playlists = new JSONArray();
                    playlists.put(playlist);
                } else if(f_email.length()>0 && f_email.equals(o_email) && !f_artist_name.equals(o_artist_name)) {
                    track.put("playlists",playlists);
                    tracks.put(track);
                    artist.put("tracks",tracks);
                    artists.put(artist);

                    //Initialize new artist
                    playlists = new JSONArray();
                    playlists.put(playlist);
                    tracks = new JSONArray();
                    track = getTrackJson(record);
                    artist = getArtistJson(record);
                } else if(f_email.length()>0 && !f_email.equals(o_email)) {
                    track.put("playlists",playlists);
                    tracks.put(track);
                    artist.put("tracks",tracks);
                    artists.put(artist);
                    user.put("artists",artists);
                    saveToFile(user,actualDate);

                    //Initialize new user
                    playlists = new JSONArray();
                    playlists.put(playlist);
                    tracks = new JSONArray();
                    track = getTrackJson(record);
                    artist = getArtistJson(record);
                    artists=new JSONArray();
                    user = getUserJson(record,actualDate);
                } else if(f_email.length()==0 && o_email.length()>0) {

                    //Initial record
                    playlists = new JSONArray();
                    playlists.put(playlist);
                    track = getTrackJson(record);
                    tracks = new JSONArray();
                    artist = getArtistJson(record);
                    artists = new JSONArray();
                    user = getUserJson(record,actualDate);
                }
                f_email=record.get("email");
                f_artist_name = record.get("artist_name");
                f_track_uri = record.get("track_uri");

            }

            result = result.getNextPage();
        }
        track.put("playlists",playlists);
        tracks.put(track);
        artist.put("tracks",tracks);
        artists.put(artist);
        user.put("artists",artists);
        System.out.println("ToHTML!!!");
        saveToFile(user,actualDate);
        //Thread.sleep(10000);
    }

    public void sendMailToMe(String msg, String subject, String whom) throws Exception {
        String sender = "swift.subscriptions@gmail.com";
        String password = "gfsniwmiqxgjoxnl";
        SSLEmail ssle = new SSLEmail();
        String to="";
        //System.out.println(user);
        //ssle.sendMail(to,subject,msg,whom);
        ssle.sendUMGMail(to,"UMG net:"+subject,msg,whom);
    }


    public void sendMailJSON(JSONObject user, String whom) throws Exception {
        String sender = "swift.subscriptions@gmail.com";
        String password = "gfsniwmiqxgjoxnl";
        SSLEmail ssle = new SSLEmail();
        JsonToHTML jth = new JsonToHTML();
        jth.prepTemplate(jth.readTemplateFile());
        String to = user.getString("email");
        String reportdate = user.getString("reportdate");
        //to="alexey.rudenko@umusic.com";
        String subject = "TEST: Track Activity Report";
        //System.out.println(user);
        String body = jth.processJson(user,reportdate);
        jth.saveHTML("resources/email_result_test.html",body);
        //ssle.sendMail(sender,password,to,subject,body,whom);
        ssle.sendUMGMail(to,"UMG net:"+subject,body,whom);
    }

    public void sendMailJSON2(JSONObject user,String whom) throws Exception {
        SSLEmail ssle = new SSLEmail();
        JsonTemplateToHTML jth = new JsonTemplateToHTML();
        String to = user.getString("email");
        String reportdate = user.getString("reportdate");
        //to="alexey.rudenko@umusic.com";
        String subject = "TEST: Track Activity Report";
        //System.out.println(user);
        String body = jth.processJson(user,reportdate);
        jth.saveHTML("resources/email_result_test.html",body);
        ssle.sendUMGMail(to,subject,body,whom);
    }

    public void saveToFile(JSONObject user, String actualDate) throws Exception {
        if(to_email)
            sendMailJSON(user,whom);

        if(to_json) {
            System.out.println("TO_JSON");
            ArrayList<String> output = new ArrayList<String>();
            output.add(user.toString());
            //Never print "data"!!!!
            //System.out.println(user);
            String[] args = {"--project="+project,"--runner="+runner};
            String output_bucket = "umg-swift-trends-alerts-triggers";
            PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
            options.setTempLocation("gs://"+project+"/temp/dataflow");
            System.out.println("Runner=" + options.getRunner());
            // Create the Pipeline object with the options we defined above.
            String output_file = "gs://" + project + "/" + output_bucket + "/alert_triggers/email_" + actualDate + "_" + user.getString("email") + "/" + "mail.json";
            System.out.println("Project="+project);
            System.out.println("Output file = "+output_file);
            Pipeline pipeline = Pipeline.create(options);
            pipeline.apply(Create.of(output)).setCoder(StringUtf8Coder.of())
                    .apply(TextIO.write().to(output_file));
            System.out.println("Start piping");
            pipeline.run().waitUntilFinish();
            System.out.println("End of piping");
        }
    }

    private JSONObject getPlaylistJson(HashMap<String,String> record) {
        JSONObject playlist = new JSONObject();
        playlist.put("name",record.get("name"));
        playlist.put("playlist_uri",record.get("playlist_uri"));
        playlist.put("owner_id",record.get("owner_id"));
        playlist.put("country",record.get("country"));
        playlist.put("followers",record.get("followers"));
        playlist.put("position",record.get("position"));
        playlist.put("action_type",record.get("action_type"));
        //playlist.put("streams",record.get("streams"));
        //playlist.put("estimated_streams",record.get("estimated_streams"));
        return playlist;
    }

    private JSONObject getTrackJson(HashMap<String,String> record) {
        JSONObject track = new JSONObject();
        track.put("product_title",record.get("product_title"));
        track.put("track_uri",record.get("track_uri"));
        track.put("track_image",record.get("track_image"));
        return track;
    }

    private JSONObject getArtistJson(HashMap<String,String> record) {
        JSONObject artist = new JSONObject();
        artist.put("artist_name",record.get("artist_name"));
        artist.put("artist_image",record.get("artist_image"));
        artist.put("artist_uri",record.get("artist_uri"));
        return artist;
    }

    private JSONObject getUserJson(HashMap<String,String> record, String actualDate) {
        JSONObject user = new JSONObject();
        user.put("firstname",record.get("firstname"));
        user.put("email",record.get("email"));
        user.put("reportdate",actualDate);
        return user;
    }

    private void printRow(List<FieldValue> row) {
        for(int j=0;j<row.size();j++) {
            System.out.print(" "+row.get(j).getValue());
        }
        System.out.println("");
    }

    private HashMap getRecord(List<FieldValue> row) throws Exception{
        HashMap<String,String> result = new HashMap<String,String>();
        String[] ss = fields.split(",");
        for(int i=0; i<ss.length;i++) {
            //System.out.println(ss[i]+"   "+row.get(i).getValue());
            if(row.get(i).getValue()!=null) {
                result.put(ss[i], row.get(i).getValue().toString());
            } else {
                result.put(ss[i], "in process");
            }
        }
        return result;
    }

    public String getToday() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public String getDaysAgo(String day, int daysago) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        if (day!=null) {
            date = dateFormat.parse(day);
        }
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, daysago);
        Date newDate = cal.getTime();
        String newDateStr = dateFormat.format(newDate);
        return newDateStr;
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Start process");
        SendTrackAction sta = new SendTrackAction();
        String dataSet="swift_alerts";
        String tableName="playlist_track_action";
        String executionDate=sta.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--project")) {
                String project = args[i + 1];
                sta.setProject(project);
            }
            if(args[i].equals("--runner")) {
                String runner=args[i+1];
                sta.setRunner(runner);
            }
            if(args[i].equals("--dataSet")) dataSet=args[i+1];
            if(args[i].equals("--tableName")) tableName=args[i+1];
            if(args[i].equals("--executionDate")) executionDate=args[i+1];
            if(args[i].equals("--email")) {
                sta.to_email=true;
                sta.whom=args[i+1];
            }
            if(args[i].equals("--json")) sta.to_json=true;

        }

        //executionDate="2017-09-05"; // Test date
        long start = System.currentTimeMillis();
        sta.executeQueueGeneral(dataSet, tableName, executionDate);
        System.out.println("End of process="+(System.currentTimeMillis()-start)/1000+" sec");
    }
}
