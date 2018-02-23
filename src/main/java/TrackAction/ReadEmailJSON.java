package TrackAction;

//import com.google.cloud.bigquery.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.*;
import org.json.JSONObject;


public class ReadEmailJSON {
    public String project="umg-dev";
    public String dataSet="swift_alerts";
    public String tableName="playlist_track_action";
    //private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private String fields="firstname,email,artist_name,artist_uri,product_title,track_uri,name,playlist_uri,owner_id,country,followers,position,action_type,artist_image,track_image,'{actualDate}' as reportdate";

    void executeQueueGeneral(String executionDate) throws Exception{
        String actualDate=getDaysAgo(executionDate,-1);
        //executePipe(actualDate);
    }
/*
    void executePipe(String actualDate) {
        try {
            String[] args = {"--project=" + project};
            com.google.cloud.dataflow.sdk.options.PipelineOptions options = com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
            options.setTempLocation("gs://umg-dev/temp");
            // Create the Pipeline object with the options we defined above.
            com.google.cloud.dataflow.sdk.Pipeline pipeline = com.google.cloud.dataflow.sdk.Pipeline.create(options);
            //org.apache.beam.sdk.Pipeline pipeline2 = org.apache.beam.sdk.Pipeline.create(options);
            System.out.println("PIPELINE STARTS");
            JsonToHTML jth = new JsonToHTML();
            String template = jth.readTemplateFile();

            com.google.cloud.dataflow.sdk.values.PCollection<String> tmpl = pipeline.apply(Create.of(template)).setCoder(StringUtf8Coder.of());
            final com.google.cloud.dataflow.sdk.values.PCollectionView<String> tmplview = tmpl.apply(View.<String>asSingleton());

            pipeline.apply(TextIO.Read.from("gs://" + project + "/" + dataSet + "/" + actualDate + "/*"))
                    //.apply("aaa",com.google.cloud.dataflow.sdk.transforms.ParDo.withSideInputs(tmplview).of(
                            .apply(com.google.cloud.dataflow.sdk.transforms.ParDo.of(
                    new DoFn<String,String>(){
                        @DoFnWithContext.ProcessElement
                        public void processElement(ProcessContext c) throws Exception {
                            //JsonToHTML jth = new JsonToHTML();
                            //SSLEmail ssle = new SSLEmail();
                            System.out.println("EMAIL to 1 USER NOW");
                            String line = c.element().toString();
//                            JSONObject user  = new JSONObject(line);
//                            System.out.println("line="+line);
//                            String sender = "swift.subscriptions@gmail.com";
//                            String password = "gfsniwmiqxgjoxnl";
//
//                            String to = user.getString("email");
//                            String reportdate = user.getString("reportdate");
//                            //to="alexey.rudenko@umusic.com";
//                            String subject = "TEST: Track Activity Report";
//                            //System.out.println(user);
//                            System.out.println("EMAIL SENDING 1");
//                            //JsonToHTML jth = new JsonToHTML();
//                            //String template = c.sideInput(tmplview).toString();
//String template="";
//                            String body = jth.processJson(user,reportdate);
//                            //String body2 = user.toString();
//                            //jth.saveHTML("resources/email_result_test.html",body);
//                            System.out.println("EMAIL SENDING 2");
//
//                            ssle.sendMail(sender,password,to,subject,body);
                            System.out.println("EMAIL SENT");
                            c.output("");
                        }
                    })
                    )
            ;
            pipeline.run();
            System.out.println("PIPELINE ENDED");
        } catch (Pipeline.PipelineExecutionException e) {
            System.out.println(actualDate+" - no email to send!");
        }
    }*/

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
        ReadEmailJSON rej = new ReadEmailJSON();
        String project="umg-dev";
        String dataSet="swift_alerts";
        String tableName="playlist_track_action";
        String executionDate=rej.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--project")) rej.project=args[i+1];
            if(args[i].equals("--dataSet")) rej.dataSet=args[i+1];
            if(args[i].equals("--tableName")) rej.tableName=args[i+1];
            if(args[i].equals("--executionDate")) executionDate=args[i+1];

        }
        //executionDate="2017-09-05"; // Test date
        rej.executeQueueGeneral(executionDate);
        System.out.println("End of process");
    }
}

