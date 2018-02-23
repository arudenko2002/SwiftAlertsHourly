package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;

public class MDBParser extends DoFn<Document,TableRow> {
    Boolean hourly=false;
    public MDBParser(Boolean hourly) {
        this.hourly=hourly;
    }
    Boolean checkRecord(JSONObject obj) {
        Boolean result=false;
        if(!obj.keySet().contains("firstName")
                || !obj.keySet().contains("lastName")
                || !obj.keySet().contains("email")
                || !obj.keySet().contains("active")
                || (obj.keySet().contains("active")&&!obj.getBoolean("active"))
                || !obj.keySet().contains("applications")
                || obj.getJSONArray("applications")==null
                || (obj.getJSONArray("applications")!=null && obj.getJSONArray("applications").length()==0)
                ) return false;
        JSONArray apps = obj.getJSONArray("applications");
        for(int i=0; i< apps.length();i++) {
            JSONObject app = apps.getJSONObject(i);
            if(
                    app.keySet().contains("name")
                    && app.getString("name")!=null
                    && app.getString("name").contains("swiftTrends")
                    && app.keySet().contains("active")
                    && app.getBoolean("active")
                    && app.keySet().contains("followedArtists")
                    && app.getJSONArray("followedArtists")!=null
                    && app.getJSONArray("followedArtists").length()>0
                    && app.keySet().contains("subscriptions")
                    && app.getJSONArray("subscriptions")!=null
                    && app.getJSONArray("subscriptions").length()>0
                    &&
                            (
                                    (!app.keySet().contains("emailFrequency") && !this.hourly)
                                    ||(app.keySet().contains("emailFrequency") && app.getString("emailFrequency").equals("hourly") && this.hourly)
                                    ||(app.keySet().contains("emailFrequency") && app.getString("emailFrequency").equals("daily") && !this.hourly)
                            )
                    ) {
                JSONArray subscriptions = app.getJSONArray("subscriptions");
                for(int j=0; j<subscriptions.length();j++) {
                    JSONObject subscription= subscriptions.getJSONObject(j);
                    if(subscription.keySet().contains("name")
                            && subscription.getString("name").equals("playlistAddDrop")
                            && subscription.keySet().contains("value")
                            && subscription.getBoolean("value")) {
                                return true;
                    }
                }
            }
        }
        return result;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        Document line = c.element();
        JSONObject obj = new JSONObject(line);
        if(checkRecord(obj))
        {
            String userId = line.get("_id").toString();
            String firstName = obj.getString("firstName");
            String lastName = obj.getString("lastName");
            String email = obj.getString("email");
            JSONArray apps = obj.getJSONArray("applications");
            for(int i=0; i< apps.length();i++) {
                JSONObject app = apps.getJSONObject(i);
                if (
                        app.keySet().contains("name")
                                && app.getString("name") != null
                                && app.getString("name").contains("swiftTrends")) {
                    //JSONObject o = (JSONObject) obj.getJSONArray("applications").getJSONObject(0);
                    JSONArray conopusids = app.getJSONArray("followedArtists");
                    for (int j = 0; j < conopusids.length(); j++) {
                        //System.out.println("UserID="+userId);
                        TableRow tr = new TableRow();
                        tr.set("userId", userId);
                        tr.set("firstName", firstName);
                        tr.set("lastName", lastName);
                        tr.set("email", email);
                        String conopus_id = conopusids.getJSONObject(j).get("_id").toString();
                        tr.set("conopus_id", conopus_id);
                        c.output(tr);
                        System.out.println(userId + "," + firstName + "," + lastName + "," + email + "," + conopus_id);
                    }
                }
            }
        }
    }

/*
    @ProcessElement
    public void processElementOLD(ProcessContext c) throws Exception {
        Document line = c.element();
        JSONObject obj = new JSONObject(line);
        if(checkRecord(obj))
        {
            String userId = line.get("_id").toString();
            String firstName = obj.getString("firstName");
            String lastName = obj.getString("lastName");
            String email = obj.getString("email");
            JSONObject o = (JSONObject) obj.getJSONArray("applications").getJSONObject(0);
            JSONArray conopusids = o.getJSONArray("followedArtists");
            for (int j = 0; j < conopusids.length(); j++) {
                //System.out.println("UserID="+userId);
                TableRow tr = new TableRow();
                tr.set("userId",userId);
                tr.set("firstName", firstName);
                tr.set("lastName", lastName);
                tr.set("email", email);
                String conopus_id = conopusids.getJSONObject(j).get("_id").toString();
                tr.set("conopus_id", conopus_id);
                c.output(tr);
                System.out.println(userId+","+firstName+","+lastName+","+email+","+conopus_id);
            }
        }
    }*/
}
