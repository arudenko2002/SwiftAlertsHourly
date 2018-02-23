package TrackAction;

import com.google.cloud.bigquery.*;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.beam.sdk.io.TextIO;
import com.google.api.services.bigquery.model.TableRow;

import static java.lang.Boolean.parseBoolean;

public class SaveBQTableAsJson {
    public String project="umg-dev";
    public String runner="DirectRunner";
    //public String dataSet="swift_alerts";
    //public String tableName="playlist_track_action";
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private String fields="firstname,email,artist_name,artist_uri,isrc,name,playlist_uri,owner_id,country,followers,position,action_type,max(track_name) as product_title,max(track_uri) as track_uri,max(track_image) as track_image,max(artist_image) as artist_image,max('{actualDate}') as reportdate";
    //private String fields="firstname,email,artist_name,artist_uri,isrc,name,playlist_uri,owner_id,country,followers,position,action_type,product_title,max(track_uri) as track_uri,max(track_image) as track_image,max(artist_image) as artist_image,max('{actualDate}') as reportdate";
    public String whom="justtome";
    private String[] arguments=null;
    Boolean hourly=false;
    DateDaysAgo datedayago = new DateDaysAgo();

    public SaveBQTableAsJson(String[] args) {
        arguments=args;
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found";
    }

    void executeQueueGeneral(String executionDate) throws Exception{
        this.hourly = Boolean.parseBoolean(getArgument("--hourly"));
        String actualDate=datedayago.getDaysAgo(executionDate,-1);
        String partition=actualDate;
        if(this.hourly) {
            //partition = datedayago.dateToPartition(datedayago.getHoursAgo(executionDate, -1)).replaceAll("-", "");
            partition = datedayago.dateToPartition(datedayago.getActualDatePeriodHourly(executionDate,0));
            //actualDate = datedayago.getHoursAgo(executionDate,-1);
            //actualDate = datedayago.getPeriodHourly(actualDate);
            actualDate = datedayago.getActualDatePeriodHourly(executionDate,0);
        }
        String source=getArgument("--playlist_track_action");
        if(this.hourly)
            source += "_hourly";
        else
            source += "_daily";
        String query ="SELECT "+fields.replace("{actualDate}",actualDate)+" FROM `"+source+"` WHERE _PARTITIONTIME = '"+partition+"' GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12 ORDER BY email,artist_name,product_title";
        executePipe(query,actualDate);
    }

    void executePipe(String sql, String actualDate) throws Exception {
        System.out.println(sql);
        String[] args={"--project="+getArgument("--project"),"--runner="+runner
        //        ,"--network=umg-xpn","--zone=us-central1-c"
        };

        String whom = getArgument("--whom");
        String gmail= getArgument("--gmail");
        Boolean alsome=parseBoolean(getArgument("--alsome"));

        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation(getArgument("--temp_directory"));
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql).usingStandardSql();
        pipeline.apply(readBigQuery)
            .apply(ParDo.of(new MapWithKey()))
            .apply(GroupByKey.<String, TableRow>create())
            .apply(ParDo.of(new ParDoMakeJSON()))
            //.apply(ParDo.of(new SendJSON(getArgument("--whom"),Boolean.parseBoolean(getArgument("--gmail")),Boolean.parseBoolean(getArgument("--alsome")))))
            //.apply(Combine.globally(new JsonListOutput()))
            //.apply(ParDo.of(new ParDoSendJsonEmails(getArgument("--whom"),Boolean.parseBoolean(getArgument("--gmail")))))
            //.apply(TextIO.write().to(getArgument("--mail_alerts_output")+actualDate+"/mail_alerts_"+actualDate+".json"))
                .apply(Combine.globally(new ParDoCombineLines()))
                .apply(ParDo.of(new ParDoSendEmail(whom,gmail,alsome)))
        ;
        pipeline.run().waitUntilFinish();
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
        SaveBQTableAsJson sta = new SaveBQTableAsJson(args);
        String executionDate=sta.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--runner")) sta.runner=args[i+1];
            if(args[i].equals("--executionDate")) executionDate=args[i+1];
        }
        long start=System.currentTimeMillis();
        sta.executeQueueGeneral(executionDate);
        System.out.println("End of process "+(System.currentTimeMillis()-start)/1000+" sec");
    }
}



