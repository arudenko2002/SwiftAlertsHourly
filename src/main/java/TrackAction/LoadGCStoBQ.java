package TrackAction;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;

public class LoadGCStoBQ {
    String executionDate="";
    String actualDate="";
    String project="";
    String dataSet="";
    String destinationTable="";
    String tempDirectory="";
    String inputFile="";
    String runner="DirectRunner";
    String[] arguments=null;
    TableSchema schema=null;
    ArrayList<String> schemaStructure=null;
    Boolean hourly=false;
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private DateDaysAgo dda = new DateDaysAgo();

    public LoadGCStoBQ() {}

    public void setLoadGCStoBQ(String[] args) throws Exception {
        arguments=args;
        String[] destTable=getArgument("--destination_table").split("\\.");
        this.hourly=Boolean.parseBoolean(getArgument("--hourly"));
        project=destTable[0];
        dataSet=destTable[1];
        destinationTable=destTable[2];
        if(this.hourly)
            destinationTable += "_hourly";
        else
            destinationTable += "_daily";
        tempDirectory=getArgument("--temp_directory");
        runner=getArgument("--runner");
        executionDate = getArgument("--executionDate");
        if(this.hourly) {
            //actualDate = dda.getHoursAgo(executionDate, -1);
            //actualDate = dda.getPeriodHourly(actualDate).replace(" ","_").replace(":","-");
            actualDate=dda.getActualDatePeriodHourly(executionDate,0).replace(" ","_").replace(":","-");
        } else {
            actualDate = dda.getDaysAgo(executionDate, -1);
        }
        System.out.println("actualDate="+actualDate);
        inputFile=getArgument("--input_file").replace("{actualDate}",actualDate);
        if(this.hourly)
            inputFile = inputFile.replace("{Hourly}","_Hourly");
        else
            inputFile = inputFile.replace("{Hourly}","_Daily");
        ReadFileLine rfl = new ReadFileLine();
        schemaStructure=rfl.readFileLine(getArgument("--schema"));
        System.out.println("inputFile="+inputFile);
        System.out.println("executionDate="+executionDate);
        System.out.println("actualDate="+actualDate);
        System.out.println("destinationTable="+ project+"."+dataSet+"."+destinationTable);
        List<TableFieldSchema> fields = new ArrayList<>();
        for(int i=0; i<schemaStructure.size();i++) {
            String s = schemaStructure.get(i);
            String[] ss  = s.split(":");
            fields.add(new TableFieldSchema().setName(ss[0]).setType(ss[1]));
        }
        schema = new TableSchema().setFields(fields);
        //this.hourly=Boolean.parseBoolean(getArgument("--hourly"));
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found ";
    }

    public boolean checkTableExists() throws Exception{
        String exec_sql="SELECT count(1) as count FROM `"+project+"."+dataSet+".__TABLES_SUMMARY__` WHERE table_id = '"+destinationTable+"'";
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
                    //System.out.println(" table="+project+"."+dataSet+"."+tableName+" does not exist");
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

    void createTable() throws Exception{
        TableId tableId = null;
        if (checkTableExists()) {
            System.out.println("Table "+destinationTable+" already exists, exiting...");
            return;
        }
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataSet, destinationTable);
        } else {
            tableId = TableId.of(dataSet, destinationTable);
        }

        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
                ;

        TableDefinition tableDefinition = builder.build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        System.out.println("Table created");
    }

    public void execute() throws Exception {
        createTable();
        System.out.println("Start process");

        // Create a PipelineOptions object.
        String[] args = {"--project="+project,"--runner="+runner};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        options.setTempLocation(tempDirectory);
        //options.setRunner(DataflowRunner.class);

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);
        //inputFile="gs://umg-dev/swift_alerts/trackHistoryAPI_2017-11-30/track_history_2017-11-30.csv*";
        //inputFile="gs://umg-alexey-test/source-directory/streams.gz";
        System.out.println("Input fs: "+inputFile);
        TextIO.Read readFile = TextIO.read().from(inputFile);
        String partition="";
        if(hourly) {
            partition = dda.dateToPartition(actualDate).replace("-", "");
            //partition=dda.dateToPartition(dda.getActualDatePeriodHourly(executionDate,0)).replace("-", "");
        }
        else
            partition = actualDate.replace("-","");

        String destination_table=project+":"+dataSet+"."+destinationTable+"$"+partition;
        System.out.println("dest_table="+destination_table+"   ex.date="+executionDate);
        //destination_table=project+":"+dataSet+"."+destinationTable;
        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to(destination_table)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        ;

        p.apply(readFile)
                .apply(ParDo.of(new ParDoStringParser(schemaStructure))) //schemafields
                .apply(writeBigQuery)
        ;
        //System.exit(0);
        p.run().waitUntilFinish();
        System.out.println("End of process");
    }

    public static void main(String args[]) throws Exception{
        Boolean hourly=false;
        for(int i=0; i<args.length;i++) {
            if(args[i].equals("--hourly")) {
                hourly = Boolean.parseBoolean(args[i+1]);
            }
        }
        System.out.println("Start process");
        LoadGCStoBQ loadgcs2bq = new LoadGCStoBQ();
        loadgcs2bq.setLoadGCStoBQ(args);
        long start=System.currentTimeMillis();
        loadgcs2bq.execute();
        System.out.println("End of process "+(System.currentTimeMillis()-start)/1000+" sec");
    }
}
