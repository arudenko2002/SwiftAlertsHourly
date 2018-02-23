package TrackAction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class ReadJSONEmailJSON {
    public String project="umg-dev";
    public String runner="DirectRunner";
    public String dataSet="swift_alerts";
    public String tableName="playlist_track_action";
    private String fields="firstname,email,artist_name,artist_uri,product_title,track_uri,name,playlist_uri,owner_id,country,followers,position,action_type,artist_image,track_image,'{actualDate}' as reportdate";
    private String[] arguments=null;

    public ReadJSONEmailJSON(String[] args) {
        arguments=args;
    }

    void executeQueueGeneral(String executionDate) throws Exception{
        String actualDate=getDaysAgo(executionDate,-1);
        executePipe(actualDate);
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found";
    }

    void executePipe(String actualDate) throws Exception{
        try {
            String[] args = {"--project=" + project, "--runner="+runner};
            PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
            options.setTempLocation("gs://umg-dev/temp");
            // Create the Pipeline object with the options we defined above.
            Pipeline pipeline = Pipeline.create(options);
            //org.apache.beam.sdk.Pipeline pipeline2 = org.apache.beam.sdk.Pipeline.create(options);
            System.out.println("PIPELINE STARTS");

            String source_files = "gs://" + project + "/" + dataSet + "/" + actualDate + "/*";
            System.out.println(source_files);
            pipeline.apply(TextIO.read().from(source_files))
                    .apply("sending_emails",ParDo.of(
                            new SendJSON(getArgument("--whom"),Boolean.parseBoolean(getArgument("--gmail")),Boolean.parseBoolean(getArgument("--alsome")))
                    ))
            ;
            pipeline.run();
            System.out.println("PIPELINE ENDED");
        } catch (Pipeline.PipelineExecutionException e) {
            System.out.println(actualDate+" - no email to send!  Pipe failed");
            e.printStackTrace();
        }
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
        ReadJSONEmailJSON rej = new ReadJSONEmailJSON(args);
        String project="umg-dev";
        String dataSet="swift_alerts";
        String tableName="playlist_track_action";
        String executionDate=rej.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--project")) rej.project=args[i+1];
            if(args[i].equals("--runner")) rej.runner=args[i+1];
            if(args[i].equals("--dataSet")) rej.dataSet=args[i+1];
            if(args[i].equals("--tableName")) rej.tableName=args[i+1];
            if(args[i].equals("--executionDate")) executionDate=args[i+1];

        }
        //executionDate="2017-09-05"; // Test date
        rej.executeQueueGeneral(executionDate);
        System.out.println("End of process");
    }
}


