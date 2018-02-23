package TrackAction;


import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CreateTablePartition {
    String project="umg-dev";
    String runner="DirectRunner";
    //String dataSet = "swift_alerts";
    //String tableName="playlist_track_action";
    String query="SELECT * FROM ";
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    String[] arguments=null;
    DateDaysAgo datedayago = new DateDaysAgo();
    Boolean hourly = false;

    public CreateTablePartition(String project, String runner,String[] args){
        this.project=project;
        this.runner=runner;
        this.arguments=args;
        this.hourly = Boolean.parseBoolean(getArgument("--hourly"));
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found ";
    }

    public boolean checkTableExists(String dataSet, String tableName) throws Exception{
        String[] args={"--project"+project,"--runner="+runner};

        String exec_sql="SELECT count(1) as count FROM `"+project+"."+dataSet+".__TABLES_SUMMARY__` WHERE table_id = '"+tableName+"'";
        //System.out.println(" project="+project+"  ds="+dataSet+"  tableName="+tableName);
        System.out.println(exec_sql);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(exec_sql)
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
        QueryResult qresult = response.getResult();
        // Print all pages of the results.
        ArrayList<String> output = new ArrayList<String>();
        String result = "";
        while (qresult != null) {
            for (List<FieldValue> row : qresult.iterateAll()) {
                String fn0 = row.get(0).getValue().toString();
                long fv0 = row.get(0).getLongValue();

                System.out.println( "number of records="+fv0+" fn0="+fn0);
                if(fv0==0) {
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" does not exist");TODO remove return false
                    return false;
                } else {
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" exists");
                    return true;
                }
            }
            qresult = qresult.getNextPage();
        }

        return false;
    }

    void createTable(String dataSet,String tableName) throws Exception{
        TableId tableId = null;
        if (checkTableExists(dataSet,tableName)) {
            System.out.println("Table "+tableName+" already exists, exiting...");
            return;
        }
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataSet, tableName);
        } else {
            tableId = TableId.of(dataSet, tableName);
        }

        List<Field> fields = new ArrayList<>();
        fields.add(Field.of("transaction_date", Field.Type.timestamp()));
        fields.add(Field.of("product_id", Field.Type.string()));
        fields.add(Field.of("sales_country_code", Field.Type.string()));
        Schema schema = Schema.of(fields);

        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));

        TableDefinition tableDefinition = builder.build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        System.out.println("Table created");
    }

    void executeQueue(String queue, String dataSet, String tableName,String actualDate) throws Exception{
        //System.out.println(queue);
        System.out.println("ActualDate="+actualDate+"  Project="+project+"  dataSet="+dataSet+"  tableName="+tableName+"$"+actualDate);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(queue)
                        .setDestinationTable(TableId.of(project,dataSet, tableName+"$"+actualDate))
                        .setUseLegacySql(false)
                        .setFlattenResults(true)
                        .setAllowLargeResults(true)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        //.setUseQueryCache(false)
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)

                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();
        System.out.println("END OF QUERY");
        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
/*
        // Get the results.
        QueryResponse response = bigquery.getQueryResults(jobId);
        QueryResult result = response.getResult();
        System.out.println("EEEEEEEEEENDDDDDDDDDDDD OF QUERY OF RESULT");
        // Print all pages of the results.
        ArrayList<String> output = new ArrayList<String>();
        int counter=0;
        while (result != null) {
            counter++;
            System.out.println("RESULT, PAGE="+counter+"   total_items="+result.getTotalRows());
            int counter2=0;
            for (List<FieldValue> row : result.iterateAll()) {
                System.out.print("iter_items="+counter2++);
                System.out.print(":   ");
                StringBuffer sb = new StringBuffer();
                //System.out.println("iter2 start");
                for(int i=0; i<row.size();i++) {
                    //System.out.println("iter3 start");
                    if(row.get(i).getValue()!=null) {
                        //System.out.println("WE ARE HERE "+i);
                        String v = row.get(i).getValue().toString();
                        System.out.print(v + ",");
                        sb.append(row.get(i).getValue()+",");
                    } else {
                        System.out.print("null,");
                        sb.append("null,");
                    }
                }
                System.out.println(": End of line");
                output.add(sb.toString());
            }
            System.out.println("PAGE OUT");
            result = result.getNextPage();
        }
*/
        System.out.println("END OF MAJOR QUERY");
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

    private String parseSQL(String sql) {
        String result=sql;
        String extension="_daily";
        if(this.hourly)
            extension="_hourly";
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals("--from_mongodb_users"))result=result.replace("{from_mongodb_users}",arguments[i+1]+extension);
            if(arguments[i].equals("--artist_track_images"))result=result.replace("{artist_track_images}",arguments[i+1]);
            if(arguments[i].equals("--playlist_geography"))result=result.replace("{playlist_geography}",arguments[i+1]+extension);
            if(arguments[i].equals("--product"))result=result.replace("{product}",arguments[i+1]);
            if(arguments[i].equals("--playlist_track_history"))result=result.replace("{playlist_track_history}",arguments[i+1]+extension);
            if(arguments[i].equals("--playlist_history"))result=result.replace("{playlist_history}",arguments[i+1]);
            if(arguments[i].equals("--streams"))result=result.replace("{streams}",arguments[i+1]);
            if(arguments[i].equals("--canopus_resource"))result=result.replace("{canopus_resource}",arguments[i+1]);
            if(arguments[i].equals("--canopus_name"))result=result.replace("{canopus_name}",arguments[i+1]);
            if(arguments[i].equals("--playlist_track_action"))result=result.replace("{playlist_track_action}",arguments[i+1]+extension);
            //if(arguments[i].equals("--executionDate"))result=result.replace("{ExecutionDate}",arguments[i+1]);
        }
        return result;
    }

    private String getSQL(String executionDate) {
        String executionDateMinusOne = datedayago.getDaysAgo(executionDate,-1);
        String executionDateMinusTwo = datedayago.getDaysAgo(executionDate,-2);
        String sqlfile="insert2.sql";
        if(this.hourly) {
            //executionDateMinusOne = datedayago.dateToPartition(datedayago.getPeriodHourly(datedayago.getHoursAgo(executionDate,-1)));
            executionDateMinusOne = datedayago.dateToPartition(datedayago.getActualDatePeriodHourly(executionDate,0));
            //executionDateMinusOne = datedayago.getActualDatePeriodHourly(executionDate,0);
            //executionDateMinusTwo = datedayago.dateToPartition(datedayago.getPeriodHourly(datedayago.getHoursAgo(executionDate,-2)));
            executionDateMinusTwo = datedayago.dateToPartition(datedayago.getActualDatePeriodHourly(executionDate,-1));
            //executionDateMinusTwo = datedayago.getActualDatePeriodHourly(executionDate,-1);
            sqlfile="insert3.sql";
        }
        System.out.println("executionDate="+executionDate+"  SQLFILE="+sqlfile+"  period1="+executionDateMinusOne+"  period2="+executionDateMinusTwo);
        try {
            InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream(sqlfile);
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

    public void procedure(String executionDate) throws Exception{
        //String executionDate = getArgument("--executionDate");
        System.out.println("Execution Date="+executionDate);
        String sql = getSQL(executionDate);
        String exec_sql = "SELECT * FROM get_details_with_artist_track_images";
        sql = sql+"\n"+exec_sql;
        System.out.println(sql);
        String partition = getDaysAgo(executionDate,-1).replaceAll("-","");
        if(this.hourly)
            partition = datedayago.dateToPartition(datedayago.getActualDatePeriodHourly(executionDate,0)).replaceAll("-","");
        System.out.println(exec_sql);
        String playlist_track_action = getArgument("--playlist_track_action");
        String[] s = playlist_track_action.split("\\.");
        String dataSet = s[1];
        String tableName = s[2];
        if(this.hourly)
            tableName += "_hourly";
        else
            tableName += "_daily";

        createTable(dataSet,tableName);
        executeQueue(sql, dataSet, tableName, partition);
    }

    public void enrichDistinctTrackURI(String executionDate) throws Exception {
        //DateDaysAgo dda = new DateDaysAgo();
        //String executionDate = getArgument("--executionDate");
        String actualDate = datedayago.getDaysAgo(executionDate,-1).replaceAll("-","");
        System.out.println("ActualDate="+actualDate);
        String sql = getSQL(executionDate);
        String[] args={"--project="+project,"--runner="+runner};
        String exec_sql="SELECT * FROM fill_tracks_with_artist_track_images";
        System.out.println(sql+"\n"+exec_sql);
        System.out.println(exec_sql);
        int counter=0;
        while(true) {
            try {
                org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
                options.setTempLocation(getArgument("--temp_directory"));
                // Create the Pipeline object with the options we defined above.
                org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
                BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql + "\n" + exec_sql).usingStandardSql();
                pipeline.apply(readBigQuery).apply(ParDo.of(new BQEnrichmentDelta()));
                pipeline.run().waitUntilFinish();
                break;
            } catch (Exception e) {
                System.out.println("Error was caught, sleeping..");
                e.printStackTrace();
                Thread.sleep(60000);
                System.out.println("Waited for 60 sec, restarted");
                counter++;
                if(counter>5) break;
            }
        }
    }

    public Boolean isYesterdayBeforeYesterdayEmpty(String executionDate, String whom) throws Exception {
        DateDaysAgo dda = new DateDaysAgo();

        String sql = getSQL(executionDate);
        String[] args={"--project"+project,"--runner="+runner};
        String exec_sql="SELECT * FROM count_lines_yesterday_before_yesterday";
        String query = sql+"\n"+exec_sql;
        System.out.println(query);
        System.out.println("  project="+project);//+"  ds="+dataSet+"  tableName="+tableName);
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
        QueryResult qresult = response.getResult();
        String playlist_track_history=getArgument("--playlist_track_history");
        // Print all pages of the results.
        ArrayList<String> output = new ArrayList<String>();
        String result = "";
        while (qresult != null) {
            for (List<FieldValue> row : qresult.iterateAll()) {
                String fn0 = row.get(0).toString();
                Long fv0 = row.get(0).getLongValue();
                //String fn1 = row.get(1).toString();
                //long fv1 = row.get(1).getLongValue();
                System.out.println( "f.value="+fv0 + " f.value="+fv0);
                if(fv0==0
                        //|| fv0 < 13000000
                        ) {
                    result = "Hourly: No track records (or fewer than 13M) in "+playlist_track_history+" for execution date "+executionDate+": check last 2 days! ";
                }
            }
            qresult = qresult.getNextPage();
        }
        if(result.length()>0) {
            //Send email
            System.out.println("ERROR: "+result+"  Sending email and exiting...");
            sendEmail("ERROR: "+result,whom);
            return true;
        }
        return false;
    }

    private void sendEmail(String body, String whom) throws Exception{
        SSLEmail ssle = new SSLEmail();
        if(Boolean.parseBoolean(getArgument("--gmail"))) {
            ssle.sendMail("alexey.rudenko@umusic.com", body, body, whom);
        } else {
            ssle.sendUMGMail("alexey.rudenko@umusic.com", "UMG net:" + body, body, whom);
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Create Table with Partition");
        InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("insert2.sql");
        System.out.println(is);
        is.close();
        System.exit(0);
        CreateTablePartition ctp = new CreateTablePartition("umg-dev","DirectRunner",args);
        //String project="umg-dev";
        String dataSet="swift_alerts";
        String tableName="playlist_track_action";
        String artistTrackImages="artist_track_images";
        System.out.println(ctp.checkTableExists(dataSet,tableName));
        ctp.procedure("2017-11-13");
    }
}
