package TrackAction;

import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONArray;
import org.json.JSONObject;

public class ParDoSendJsonEmails extends DoFn<String,String> {

    String sender = "swift.subscriptions@gmail.com";
    String password = "gfsniwmiqxgjoxnl";
    String whom="justtome";
    Boolean gmail=true;
    Boolean alsome=true;

    public ParDoSendJsonEmails(String whom,Boolean gmail,Boolean alsome) {
        this.whom=whom;
        this.gmail=gmail;
        this.alsome=alsome;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        //System.out.println("STEP 3 PROBLEM IS NERE");
        SSLEmail ssle = new SSLEmail();
        int counter=0;
        while (true) {
            counter++;
            if (counter>3) {
                ssle.sendUMGMail("alexey.rudenko@umusic.com", "JSON Error in JSON Error in ParDoSendJsonEmails", "JSON Error in JSON Error in ParDoSendJsonEmails", "justtome");
                System.exit(1);
            }
            try {
                String line = c.element();
                JSONArray ja = new JSONArray(line);

                JsonToHTML jth = new JsonToHTML();
                if (ja.length() == 0) {
                    String body = "WARNING: The list of emails is empty, check the track records (last 2 days).";
                    if (gmail) {
                        ssle.sendMail("alexey.rudenko@umusic.com", body, body, "justtome");
                    } else {
                        ssle.sendUMGMail("alexey.rudenko@umusic.com", body, body, "justtome");
                    }
                }
                for (int i = 0; i < ja.length(); i++) {
                    //System.out.println("line=" + line);
                    JSONObject user = ja.getJSONObject(i);
                    jth.prepTemplate(jth.readTemplateFile());
                    String to = user.getString("email");
                    String reportdate = user.getString("reportdate");
                    String subject = "Track Activity Report";
                    String body = jth.processJson(user, reportdate);
                    if (gmail) {
                        ssle.sendMail(to, subject, body, whom);
                        if(alsome)
                            ssle.sendMail("alexey.rudenko@umusic.com", subject, body, "justtome");
                    } else {
                        ssle.sendUMGMail(to, subject, body, whom);
                        if(alsome)
                            ssle.sendUMGMail("alexey.rudenko@umusic.com", subject, body, "justtome");
                    }
                    //System.out.println("EMAIL SENT");
                }
                c.output(line);
                ssle.sendMail("alexey.rudenko@umusic.com", "Emais sent="+ja.length(), "Emais sent="+ja.length(), "justtome");
                break;
            } catch (org.json.JSONException e) {
                System.out.println("JSON Error in ParDoSendJsonEmails");
                e.printStackTrace();
                Thread.sleep(60000);
            }
        }
    }
}

