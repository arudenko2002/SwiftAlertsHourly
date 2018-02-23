package TrackAction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * An example that joins together 3 tables
 * By Alexey Rudenko
 */
public class TrackActionSubscription {
    String project="umg-dev";
    String runner="DirectRunner";
    String whom="justtome";
    public Boolean exe_mongodb=false;
    public Boolean exe_enrichment=false;
    public Boolean exe_major_sql=false;
    String[] arguments=null;
    public TrackActionSubscription(String[] args) {
        arguments=args;
    }

    void setProject(String project) {
        this.project=project;
    }

    void setRunner(String runner) {
        this.runner=runner;
    }

    public Boolean isYesterdayBeforeYesterdayNotEmpty(String executionDate) throws Exception{
        CreateTablePartition  ctp = new CreateTablePartition(project,runner,arguments);
            if(ctp.isYesterdayBeforeYesterdayEmpty(executionDate,whom)) {
            return false;
        }
        return true;
    }

    public void createTableTriggerEmail(String executionDate) throws Exception{
        CreateTablePartition  ctp = new CreateTablePartition(project,runner,arguments);
        if(exe_enrichment)
            ctp.enrichDistinctTrackURI(executionDate);//"artist_track_images");
        if(exe_major_sql) {
            System.out.println("Create the table with partitions if necessary, fill the partition.");
            ctp.procedure(executionDate); //Major SQL isd running here
        }
        System.out.println("End of the process");
    }

    public String getToday() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Start process");
        long start=System.currentTimeMillis();
        TrackActionSubscription tate = new TrackActionSubscription(args);

        //String dataSet="metadata";
        String dataSet="swift_alerts";
        String tableName="playlist_track_action";
        String artistTrackImages="artist_track_images";
        String executionDate=tate.getToday();
        for (int i=0; i< args.length;i++) {
            if(args[i].equals("--project")) {
                String project = args[i + 1];
                tate.setProject(project);
            }
            if(args[i].equals("--runner")) {
                String runner=args[i+1];
                tate.setRunner(runner);
            }

            if(args[i].equals("--executionDate")) executionDate=args[i+1];
            if(args[i].equals("--mongodb")) tate.exe_mongodb=true;
            if(args[i].equals("--enrichment")) tate.exe_enrichment=true;
            if(args[i].equals("--major_sql")) tate.exe_major_sql=true;
            if(args[i].equals("--whom")) executionDate=args[i+1];
        }

        if(tate.isYesterdayBeforeYesterdayNotEmpty(executionDate)) {
            if(tate.exe_mongodb) {
                MongoDBToBigquery mdbbq = new MongoDBToBigquery(tate.project, tate.runner,args);
                mdbbq.moveMongoDBtoBigQuery();
            }
            //executionDate="2017-09-05";
            tate.createTableTriggerEmail(executionDate);
        } else {
            System.out.println("Track data is missed.  Exiting...");
            return;
        }

        System.out.println("End of process");
        System.out.println("End of process "+(System.currentTimeMillis()-start)/1000+" sec");
    }
}


