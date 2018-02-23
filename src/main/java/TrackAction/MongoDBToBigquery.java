package TrackAction;


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

//import com.google.common.annotations.VisibleForTesting;


public class MongoDBToBigquery {
    String project="umg-dev";
    String runner="DirectRunner";
    String user  = "dizzee-np-admin";//"swift-dev-admin";
    String password = "3(8F$28qv93]nyh";//"eef9XeifIemiech8";
    String host = "35.192.123.192";//"35.197.2.144";
    int port = 27017;
    String databaseA = "dizzee-user-dev";//"sst-dizzee-dev";
    String collectionA = "users";
    String source = "admin";
    String from_mongodb_users=project+".swift_alerts.from_mongodb_users";
    String temp_directory="gs://umg-dev/temp";
    String[] arguments = null;
    Boolean hourly=false;

    String uri = "mongodb://"+user+":"+password+"@"+host+":"+port+"/?authMechanism=SCRAM-SHA-1&authSource="+source;

    public MongoDBToBigquery(String project, String runner, String[] args) {
        this.project=project;
        this.runner=runner;
        arguments=args;
        hourly = Boolean.parseBoolean(getArgument("--hourly"));
    }

    private String getArgument(String find) {
        for(int i=0;i<arguments.length;i++) {
            if(arguments[i].equals(find))return arguments[i+1];
        }
        return "Not found ";
    }

    public void mongoDBDataFlow(Pipeline pipeline) {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("userId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("firstname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lastname").setType("STRING"));
        fields.add(new TableFieldSchema().setName("email").setType("STRING"));
        fields.add(new TableFieldSchema().setName("conopus_id").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);
        if(arguments!=null) {
            String table1 = getArgument("--from_mongodb_users");
            String[] table2 = table1.split("\\.");
            from_mongodb_users=table2[0]+":"+table2[1]+"."+table2[2];
            if(this.hourly)
                from_mongodb_users += "_hourly";
            else
                from_mongodb_users += "_daily";
            if(getArgument("--mongoDB").equals("dev")) {
                devMongoDB mdb = new devMongoDB();
                uri = mdb.getURI();
                databaseA = mdb.getDatabase();
                collectionA = mdb.getCollection();
            }

            if(getArgument("--mongoDB").equals("prod")) {
                prodMongoDB mdb = new prodMongoDB();
                uri = mdb.getURI();
                databaseA = mdb.getDatabase();
                collectionA = mdb.getCollection();
            }
        }

        System.out.println(uri);
        MongoDbIO.Read readMongoDB = MongoDbIO.read()
                .withUri(uri)
                .withDatabase(databaseA)
                .withCollection(collectionA);

        BigQueryIO.Write<TableRow> writeBigQuery = BigQueryIO.writeTableRows()
                .to(from_mongodb_users)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);

        pipeline.apply(readMongoDB)
                .apply(ParDo.of(new MDBParser(this.hourly)))
                .apply(writeBigQuery)
                ;
    }

    private void sendEmail(String body, String whom) throws Exception{
        SSLEmail ssle = new SSLEmail();
        if(Boolean.parseBoolean(getArgument("--gmail"))) {
            ssle.sendMail("alexey.rudenko@umusic.com", body, body, whom);
        } else {
            ssle.sendUMGMail("alexey.rudenko@umusic.com", "UMG net:" + body, body, whom);
        }
    }

    public void moveMongoDBtoBigQuery() throws Exception{
        String[] args = {"--project="+project,"--runner="+runner};
        System.out.println("MongoDBToBigquery started");
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        if(arguments!=null)
            temp_directory=getArgument("--temp_directory");
        options.setTempLocation(temp_directory);
        //options.setRunner(DataflowRunner)
        // Create the Pipeline object with t
        // he options we defined above.
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        int counter=0;
        while(true) {
            try {
                mongoDBDataFlow(pipeline);
                pipeline.run().waitUntilFinish();
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            Thread.sleep(30000);
            counter++;
            if(counter>5) {
                System.out.println("Access to MongoDB failed!");
                sendEmail("Access to MongoDB failed!", "justtome");
                System.exit(0);
            }
        }
        System.out.println("End of MongoDBToBigquery process");
    }

    class devMongoDB {
        String user  = "dizzee-np-admin";//"swift-dev-admin";
        String password = "3(8F$28qv93]nyh";//"eef9XeifIemiech8";
        String host = "35.192.123.192";//"35.197.2.144";
        int port = 27017;
        String databaseA = "dizzee-user-dev";//"sst-dizzee-dev";
        String collectionA = "users";
        String source = "admin";
        String getURI() {
            String uri = "mongodb://"+user+":"+password+"@"+host+":"+port+"/?authMechanism=SCRAM-SHA-1&authSource="+source;
            return uri;
        }
        String getDatabase() {
            return databaseA;
        }

        String getCollection() {
            return collectionA;
        }
    }

    class prodMongoDB {
        String user  = "dizzee-p-admin";//"swift-dev-admin";
        String password = "pTo%258pURU|}%25])L";//"eef9XeifIemiech8";
        String host = "35.184.210.244";//"35.197.2.144";
        int port = 27017;
        String databaseA = "dizzee-user-prod";//"sst-dizzee-dev";
        String collectionA = "users";
        String source = "admin";
        String getURI() {
            String uri = "mongodb://"+user+":"+password+"@"+host+":"+port+"/?authMechanism=SCRAM-SHA-1&authSource="+source;
            return uri;
        }
        String getDatabase() {
            return databaseA;
        }

        String getCollection() {
            return collectionA;
        }
    }

    public static void main(String args[]) throws Exception{
        MongoDBToBigquery mdb = new MongoDBToBigquery("umg-dev","DirectRunner",args);
        mdb.moveMongoDBtoBigQuery();
    }
}
