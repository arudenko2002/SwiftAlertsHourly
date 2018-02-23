package TrackAction;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Vijay_test {
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private TableSchema getSchemaSwiftTrendsAlerts() {
        return new TableSchema().setFields(getSchemaFields());
    }



    private List<TableFieldSchema> getSchemaFields() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("conopus_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("product_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("product_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("upc").setType("STRING"));
        fields.add(new TableFieldSchema().setName("isrc").setType("STRING"));
        fields.add(new TableFieldSchema().setName("master_artist_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("master_artist").setType("STRING"));
        fields.add(new TableFieldSchema().setName("master_track_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("master_track").setType("STRING"));
        fields.add(new TableFieldSchema().setName("master_album_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("master_album").setType("STRING"));
        fields.add(new TableFieldSchema().setName("resource_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("release_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("project_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("project_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_hfm_rep_owner").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_segment").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_profit_center").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_financial_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("r2_company").setType("STRING"));
        fields.add(new TableFieldSchema().setName("r2_division").setType("STRING"));
        fields.add(new TableFieldSchema().setName("r2_label").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_sales_rep_owner").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_source_rep_owner").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sap_domestic_flag").setType("STRING"));
        fields.add(new TableFieldSchema().setName("product_release_date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("first_release_activity_week").setType("STRING"));
        fields.add(new TableFieldSchema().setName("earliest_resource_release_date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("original_release_date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("r2_family_id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("r2_Family_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("configuration_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("configuration_description").setType("STRING"));
        fields.add(new TableFieldSchema().setName("resource_version_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("release_version_title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("genre_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("genre_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sub_genre_code").setType("STRING"));
        fields.add(new TableFieldSchema().setName("sub_genre_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("product_group").setType("STRING"));
        fields.add(new TableFieldSchema().setName("product_life_cycle_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("r2_project_number").setType("STRING"));
        fields.add(new TableFieldSchema().setName("r2_project_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("project_release_date").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("first_project_activity_week").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("load_datetime").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("playlist_uri").setType("STRING"));
        fields.add(new TableFieldSchema().setName("report_date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("position").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("added_at").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("added_by").setType("STRING"));
        fields.add(new TableFieldSchema().setName("track_uri").setType("STRING"));
        fields.add(new TableFieldSchema().setName("track_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("artist_uri").setType("STRING"));
        fields.add(new TableFieldSchema().setName("artist_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("album_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("album_uri").setType("STRING"));
        fields.add(new TableFieldSchema().setName("album_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("action_type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("playlist_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("description").setType("STRING"));
        fields.add(new TableFieldSchema().setName("owner_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("owner_uri").setType("STRING"));
        fields.add(new TableFieldSchema().setName("followers").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("action_date").setType("DATE"));
        fields.add(new TableFieldSchema().setName("action_timestamp").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("streams").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("estimated_streams").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("country").setType("STRING"));
        fields.add(new TableFieldSchema().setName("artist_image").setType("STRING"));
        fields.add(new TableFieldSchema().setName("track_image").setType("STRING"));
        return fields;
    }

    private TableSchema getSchemaArtistTrackImages() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("track_uri").setType("STRING"));
        fields.add(new TableFieldSchema().setName("artist_image").setType("STRING"));
        fields.add(new TableFieldSchema().setName("track_image").setType("STRING"));
        fields.add(new TableFieldSchema().setName("last_update").setType("TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    public void moveMongoDBtoBigQuery() {
        String[] args = {""};
        System.out.println("MongoDBToBigquery startedssssss");
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp");
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        PCollection<TableRow> aggPcollection = pipeline.apply(BigQueryIO.read()
                //.named(entity.toStringShard())
                .fromQuery("SELECT * FROM metadata.spotify_playlist_details WHERE _PARTITIONTIME = TIMESTAMP(\"2017-08-04\") LIMIT 100;").usingStandardSql());
        //mongoDBDataFlow(pipeline);
        pipeline.run().waitUntilFinish();
        System.out.println("End of MongoDBToBigquery processffffffff");
    }

    public void enrichBigQuery(String executionDate) throws IOException {
        DateDaysAgo dda = new DateDaysAgo();
        String actualDate = dda.getDaysAgo(executionDate,-1).replaceAll("-","");
        System.out.println("ActualDate="+actualDate);
        InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("insert.sql");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuffer stringBuffer = new StringBuffer();
        String line = null;
        while((line =reader.readLine())!=null){
            stringBuffer.append(line).append("\n");
        }
        String[] args={""};
        String sql  = stringBuffer.toString().replace("{ExecutionDate}",executionDate);
        String exec_sql = " select a.*,b.artist_image,b.track_image from " +
                "get_user_products_trackers_details a " +
                "left join `umg-dev.swift_alerts.artist_track_images` b " +
                "ON a.track_uri=b.track_uri";
        System.out.println(sql+"\n"+exec_sql);
        //CreateTablePartition  ctp = new CreateTablePartition();
        //ctp.createTable("umg-dev","swift_trends_alerts","playlist_track_action3");
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp");
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql+"\n"+exec_sql).usingStandardSql();
        TableSchema schema = getSchemaSwiftTrendsAlerts();
        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
        .to("umg-dev:swift_alerts.playlist_track_action$"+actualDate)
        .withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        pipeline.apply(readBigQuery)
                .apply(ParDo.of(new BQEnrichmentDelta()))
                .apply(writeBigQuery)
        ;
        pipeline.run().waitUntilFinish();
    }

    public void enrichDistinctTrackURI(String executionDate) throws IOException {
        DateDaysAgo dda = new DateDaysAgo();
        String actualDate = dda.getDaysAgo(executionDate,-1).replaceAll("-","");
        System.out.println("ActualDate="+actualDate);
        InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("insert.sql");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuffer stringBuffer = new StringBuffer();
        String line = null;
        while((line =reader.readLine())!=null){
            stringBuffer.append(line).append("\n");
        }
        String[] args={""};
        String sql  = stringBuffer.toString().replace("{ExecutionDate}",executionDate);
        String exec_sql = " select a.track_uri,b.artist_image from " +
                "get_user_products_trackers_details a " +
                "left join `umg-dev.swift_alerts.artist_track_images` b " +
                "ON a.track_uri=b.track_uri " +
                "WHERE b.artist_image IS NULL GROUP BY 1,2 ";
        System.out.println(sql+"\n"+exec_sql);
        //CreateTablePartition  ctp = new CreateTablePartition();
        //ctp.createTable("umg-dev","swift_trends_alerts","playlist_track_action3");
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp");
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql+"\n"+exec_sql).usingStandardSql();
        /*
        TableSchema schema = getSchemaSwiftTrendsAlerts();
        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to("umg-dev:swift_trends_alerts.playlist_track_action$"+actualDate)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
*/
        pipeline.apply(readBigQuery)
                .apply(ParDo.of(new BQEnrichmentDelta()))
  //              .apply(writeBigQuery)
        ;
        pipeline.run().waitUntilFinish();
    }

    public void fillImageUri(String executionDate) throws IOException {
        InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("insert.sql");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuffer stringBuffer = new StringBuffer();
        String line = null;
        while((line =reader.readLine())!=null){
            stringBuffer.append(line).append("\n");
        }
        String sql  = stringBuffer.toString().replace("{ExecutionDate}",executionDate);
        String[] args={""};
        String exec_sql="SELECT DISTINCT track_uri FROM `umg-tools.metadata.spotify_playlist_tracks` a " +
                "join get_top_playlists b on a.playlist_uri = b.playlist_uri " +
                "WHERE _PARTITIONTIME>=TIMESTAMP(DATE_SUB(\"{ExecutionDate}\", INTERVAL 3 MONTH))"
                        .replace("{ExecutionDate}",executionDate);

        System.out.println(sql+"\n"+exec_sql);
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp");
        // Create the Pipeline object with the options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);

        BigQueryIO.Read readBigQuery = BigQueryIO.read().fromQuery(sql+"\n"+exec_sql).usingStandardSql();
        TableSchema schema = getSchemaArtistTrackImages();

        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to("umg-dev:swift_alerts.artist_track_images_temp")
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        pipeline.apply(readBigQuery)
                .apply(ParDo.of(new BQEnrichment()))
                .apply(writeBigQuery);

        pipeline.run().waitUntilFinish();

    }

    public boolean checkTableExists(String datasetId, String tableName) {
        Table table = bigquery.getTable(datasetId, tableName);
        if (table == null) {
            return false;
        }
        return true;
    }
/*
    void createTable(String project, String dataSet,String tableName) {
        TableId tableId = null;
        if (checkTableExists(dataSet,tableName)) {
            System.out.println(tableName+" already exists, exiting...");
            return;
        }
        if (StringUtils.isNotEmpty(project)) {
            tableId = TableId.of(project, dataSet, tableName);
        } else {
            tableId = TableId.of(dataSet, tableName);
        }

        //List<Field> fields = new ArrayList<>();
        //fields.add(Field.of("transaction_date", Field.Type.timestamp()));
        //fields.add(Field.of("product_id", Field.Type.string()));
        //fields.add(Field.of("sales_country_code", Field.Type.string()));
        //Schema schema = getSchemaSwiftTrendsAlerts();
        Schema schema = Schema.of(getSchemaFields());

        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
                .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));
        //.setSchema(schema);

        TableDefinition tableDefinition = builder.build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        Table table = bigquery.create(tableInfo);
        System.out.println("Table created");
    }
*/
/*
    void executeQueue() throws Exception {
        String today_now = ISODateTimeFormat.dateTime().print(new DateTime(org.joda.time.DateTimeZone.UTC));

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        String project="umg-dev";
        String dataSet="swift_trends_alerts";
        String tableName="artist_track_images";
        String query = "INSERT `"+project+"."+dataSet+"."+tableName+"` (track_uri,artist_image,track_image,last_update) VALUES "+
                "(\"ddddd\",\"eeeee\",\"gggggg\",\""+today_now+"\")";
        //System.out.println("ACTUAL DATE=" + actualDate + "  project=" + project + "  ds=" + dataSet + "  tableName=" + tableName + " actusalDate=" + actualDate);
        System.out.println(query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        //.setDestinationTable(TableId.of(project, dataSet, tableName))
                        .setUseLegacySql(false)
                        //.setFlattenResults(true)
                        //.setAllowLargeResults(true)
                        //.setPriority(QueryJobConfiguration.Priority.BATCH)
                        //.setUseQueryCache(false)
                        //.setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                        //.setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        System.out.println("Query executed");
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
    }*/
    public static void main(String args[]) throws Exception{
        Vijay_test mdb = new Vijay_test();
        //mdb.moveMongoDBtoBigQuery();
        //mdb.fillImageUri("2017-09-30");
        //mdb.executeQueue();
        //mdb.enrichDistinctTrackURI("2017-09-05");
        mdb.enrichBigQuery("2017-09-26");
    }
}
