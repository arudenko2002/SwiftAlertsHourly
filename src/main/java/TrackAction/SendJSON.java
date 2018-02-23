package TrackAction;

import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

public class SendJSON extends DoFn<String,String> {
        String sender = "swift.subscriptions@gmail.com";
        String password = "gfsniwmiqxgjoxnl";
        String whom="justtome";
        Boolean gmail=true;
        Boolean alsome=true;

        public SendJSON(String whom,Boolean gmail,Boolean alsome) {
                this.whom=whom;
                this.gmail=gmail;
                this.alsome=alsome;
        }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
            String line = c.element();
            JsonToHTML jth = new JsonToHTML();
            jth.prepTemplate(jth.readTemplateFile());
            SSLEmail ssle = new SSLEmail();
            JSONObject user  = new JSONObject(line);
            String to = user.getString("email");
            String reportdate = user.getString("reportdate");
            String subject = "Hourly Track Activity Report";
            String body = jth.processJson(user,reportdate);
            if (gmail) {
                    ssle.sendMail(to, subject, body, whom);
                    if(alsome)
                            ssle.sendMail("alexey.rudenko@umusic.com", subject, body, "justtome");
            } else {
                    ssle.sendUMGMail(to, subject, body, whom);
                    if(alsome)
                            ssle.sendUMGMail("alexey.rudenko@umusic.com", subject, body, "justtome");
            }
            //ssle.sendMail(sender,password,to,subject,body,"justtome");
            System.out.println("EMAIL SENT");
            c.output(line);
    }
}
