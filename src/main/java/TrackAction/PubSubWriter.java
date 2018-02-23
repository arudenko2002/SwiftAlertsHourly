package TrackAction;

import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;

public class PubSubWriter {
    String project_id=null;
    String topic_id=null;

    public PubSubWriter(String project_id, String topic_id){
        this.project_id=project_id;
        this.topic_id=topic_id;
    }
    public void writePubSub2(String email,String subject, String body) throws Exception{
        System.out.println("Writing to PUBSUB");
        String[] args={"--project=umg-swift","--runner=DirectRunner"};
        org.apache.beam.sdk.options.PipelineOptions options = org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://umg-dev/temp");
        List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");
        //List<String> input = new ArrayList<String>();
        List<PubsubMessage> input = new ArrayList<PubsubMessage>();
        Map attributes = new Hashtable();
        attributes.put("email",email);
        attributes.put("subject",subject);
        for(int i=0;i<10;i++) {
            //input.add("alexey.rudenko@umusic.com|Privet ot PubSub|"+LINES.get(i));
            //ByteString data = ByteString.copyFromUtf8(LINES.get(i));
            String data="Letter #"+i;
            PubsubMessage pubsubMessage = new PubsubMessage(data.getBytes("UTF-8"),attributes);
            input.add(pubsubMessage);
        }
        Pipeline p = Pipeline.create(options);
        PCollection<PubsubMessage> streamData = p.apply(Create.of(input));
        PubsubIO.Write<PubsubMessage> write = PubsubIO
                .writeMessages()
                //.writeStrings()
                .to("projects/umg-swift/topics/test-topic")
                //.withTimestampAttribute("2018-01-19T00:00:00Z")
                //.withIdAttribute("666")
                ;
        streamData.apply(write);
        DisplayData displayData = DisplayData.from(write);
        System.out.println(displayData.toString());
        System.out.println("sss="+displayData.items().toArray()[0]);
        p.run().waitUntilFinish();
        System.out.println("Written");
    }

    public static void main(String[] args) throws Exception {
        PubSubWriter ss = new PubSubWriter("umg-swift","test-topic");
        ss.writePubSub2("alexey.rudenko@umusic.com","Hello from PubSub","Hello User from PubSub!");
    }
}
