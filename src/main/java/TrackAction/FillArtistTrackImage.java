package TrackAction;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FillArtistTrackImage {
    String[] arguments=null;
    DateDaysAgo datedayago=new DateDaysAgo();
    public FillArtistTrackImage(String[] args) {
        arguments=args;
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found ";
    }

    private String parseSQL(String sql) {
        String result=sql;
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals("--from_mongodb_users"))result=result.replace("{from_mongodb_users}",arguments[i+1]);
            if(arguments[i].equals("--artist_track_images"))result=result.replace("{artist_track_images}",arguments[i+1]);
            if(arguments[i].equals("--playlist_geography"))result=result.replace("{playlist_geography}",arguments[i+1]);
            if(arguments[i].equals("--product"))result=result.replace("{product}",arguments[i+1]);
            if(arguments[i].equals("--playlist_track_history"))result=result.replace("{playlist_track_history}",arguments[i+1]);
            if(arguments[i].equals("--playlist_history"))result=result.replace("{playlist_history}",arguments[i+1]);
            if(arguments[i].equals("--streams"))result=result.replace("{streams}",arguments[i+1]);
            if(arguments[i].equals("--canopus_resource"))result=result.replace("{canopus_resource}",arguments[i+1]);
            if(arguments[i].equals("--canopus_name"))result=result.replace("{canopus_name}",arguments[i+1]);
            if(arguments[i].equals("--playlist_track_action"))result=result.replace("{playlist_track_action}",arguments[i+1]);
            //if(arguments[i].equals("--executionDate"))result=result.replace("{ExecutionDate}",arguments[i+1]);
        }
        return result;
    }

    private String getSQL(String executionDate) {
        String executionDateMinusOne = datedayago.getDaysAgo(executionDate,-1);
        String executionDateMinusTwo = datedayago.getDaysAgo(executionDate,-2);
        try {
            InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("insert3.sql");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            StringBuffer stringBuffer = new StringBuffer();
            String line = null;
            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }
            String sql =  stringBuffer.toString();
            String fullsql = parseSQL(sql);
            fullsql = fullsql.replace("{ExecutionDate}",executionDate);
            fullsql = fullsql.replace("{ExecutionDateMinusOne}",executionDateMinusOne);
            fullsql = fullsql.replace("{ExecutionDateMinusTwo}",executionDateMinusTwo);
            return fullsql;
        } catch(IOException e) {
            System.out.println("IOException");
            return null;
        }
    }

    public void enrichDistinctTrackURI(String executionDate) throws IOException {
        DateDaysAgo dda = new DateDaysAgo();
        String actualDate=dda.getDaysAgo(executionDate,-1);
        String sql  =getSQL(executionDate);//.replace("{ExecutionDate}",executionDate).replace("{project}",project);

        String[] args={"--project="+getArgument("--project"),"--runner="+getArgument("--runner")};
        //sql  = sql.replace("{ExecutionDate}",executionDate).replace("{project}",project);
        //String exec_sql = "SELECT * from fill_all_tracks_with_artist_track_images";
        String exec_sql = "SELECT * from get_canopused_images";
        System.out.println(sql+"\n"+exec_sql);
        String millis = ""+System.currentTimeMillis();
        String index = millis.substring(millis.length()-3);
        String outputfile = getArgument("--outputfile");
        String output = outputfile+"/imagesAPI_"+actualDate+"/images_"+actualDate+"_"+index+".csv";
        System.out.println("Output file="+output);
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation(getArgument("--temp_directory"));
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql+"\n"+exec_sql).usingStandardSql();
        pipeline.apply(readBigQuery).apply(ParDo.of(new ParDoBQEnrichment()))
                .apply(TextIO.write().to(output));
        pipeline.run().waitUntilFinish();
    }
    public static void main(String[] args) throws Exception {
        FillArtistTrackImage fati = new FillArtistTrackImage(args);
        DateDaysAgo dda = new DateDaysAgo();
        //String project="umg-dev";
        //String runner="DirectRunner";
        //String path="swift_alerts";
        String executionDate=dda.getToday();
        for (int i=0; i< args.length;i++) {
            //if(args[i].equals("--project")) project=args[i+1];
            //if(args[i].equals("--runner")) runner=args[i+1];
            if(args[i].equals("--executionDate")) executionDate=args[i+1];
        }
        fati.enrichDistinctTrackURI(executionDate);
    }
}
