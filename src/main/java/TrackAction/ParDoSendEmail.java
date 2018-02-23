package TrackAction;

//import com.sun.javafx.tools.packager.Log;
//import com.sun.media.jfxmedia.logging.Logger;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

import java.util.Hashtable;

public class ParDoSendEmail extends DoFn<String,String> {
    String whom = "justtome";
    String gmail = "UMG";
    Boolean alsome = true;
    String dividor="&&&";
    SSLEmail ssle=null;

    public ParDoSendEmail(String whom, String gmail, Boolean alsome) throws Exception {
        this.whom = whom;
        this.gmail = gmail;
        this.alsome = alsome;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String line = c.element();
        String[] lines = line.split(dividor);
        Hashtable ht = new Hashtable();

        JsonToHTML jth = new JsonToHTML();
        jth.prepTemplate(jth.readTemplateFile());
        SSLEmail ssle = new SSLEmail(gmail);
        for(int i=0; i<lines.length;i++) {
            if(ht.containsKey(lines[i])) {
                System.out.println("Reapeat "+lines[i]);
                System.exit(-100);
            }
            if(lines[i].length()>0)ht.put(lines[i],"");
        }
        ssle.sendMail("alexey.rudenko@umusic.com", "Size="+ht.size()+"  "+lines.length, "Size="+ht.size()+"  "+lines.length, "justtome");
        //System.exit(-1);
        int counter=0;
        for (Object key : ht.keySet()) {
        //for (int i = 0; i < lines.length; i++) {
            //String l = lines[i];
            String l = key.toString();
            System.out.println(""+counter+"   LINE="+l);
            //counter++;
            //if(true)continue;
            //System.out.println("line="+l);
            if(l.length()==0) continue;
            JSONObject user = new JSONObject(l);
            String to = user.getString("email");
            String reportdate = user.getString("reportdate");
            String subject = "Hourly Track Activity Report";
            String subjectforme=subject +" "+user.getString("email");
            String body = jth.processJson(user, reportdate);
            Thread.sleep(5000);
            if (gmail.startsWith("gmail")) {
                if(counter % 50==0 && counter>0) {
                    System.out.println("Restart of Session");
                    ssle.closeConnectionGmail();
                    ssle = new SSLEmail(gmail);
                }
                System.out.println("Sending");
                ssle.sendMail(to, subject, body, whom);
                if (alsome)
                    ssle.sendMail("alexey.rudenko@umusic.com", subject, body, "justtome");
            } else {
                ssle.sendUMGMail(to, subject, body, whom);
                if (alsome)
                    ssle.sendUMGMail("alexey.rudenko@umusic.com", subjectforme, body, "justtome");
            }

            System.out.println("EMAIL SENT #"+counter+" of "+ ht.size());
            counter++;
            //c.output("");
        }
        if(gmail.startsWith("gmail"))
            ssle.closeConnectionGmail();
        c.output("");
    }
}
