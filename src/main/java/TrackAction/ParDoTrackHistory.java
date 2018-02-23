package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Hashtable;

public class ParDoTrackHistory extends DoFn<TableRow,String> {
    String playlist_date="";
    public ParDoTrackHistory(String date){
        playlist_date=date;
    }

    private ArrayList<String> getTrackRecords(String response) throws Exception{
        //System.out.println(response);
        JSONObject playlist = new JSONObject(response);
        String playlist_uri = "None";
        if(!playlist.isNull("uri"))
            playlist_uri = playlist.getString("uri");
        //System.out.println("1,13. playlist_uri="+playlist_uri);

        //String playlist_date = playlist_date; //getDaysAgo(-1);
        //System.out.println("2,18. playlist_date="+playlist_date);

        String playlist_id = "None";
        if(!playlist.isNull("id"))
            playlist_id = playlist.getString("id");
        //System.out.println("14. playlist_id="+playlist_id);

        String playlist_owner = "None";
        if(!playlist.isNull("owner")
                && !playlist.getJSONObject("owner").isNull("id"))
            playlist_owner = playlist.getJSONObject("owner").getString("id");
        //System.out.println("15. playlist_owner="+playlist_owner);
        String playlist_owner_uri = "None";
        if(!playlist.isNull("owner")
                && !playlist.getJSONObject("owner").isNull("uri"))
            playlist_owner_uri = playlist.getJSONObject("owner").getString("uri");
        //System.out.println("16. playlist_owner_uri="+playlist_owner_uri);

        String playlist_name = "None";
        if(!playlist.isNull("name"))
            playlist_name = playlist.getString("name");
        playlist_name = playlist_name.replaceAll("\"","'").replace('\\',' ');
        //System.out.println("17. playlist_name="+playlist_name);
        String followers = "0";
        if(!playlist.isNull("followers")
                && !playlist.getJSONObject("followers").isNull("total"))
            followers = playlist.getJSONObject("followers").get("total").toString();
        //System.out.println("19. followers="+followers);

        ArrayList<String> ar  = new ArrayList<String>();
        if(playlist.isNull("tracks")
                || playlist.getJSONObject("tracks").isNull("items"))
            return ar;
        JSONArray tracks = playlist.getJSONObject("tracks").getJSONArray("items");
        for(int i=0; i<tracks.length();i++) {
            JSONObject track = tracks.getJSONObject(i);
            String track_add_by = "None";
            if(!track.isNull("added_by") &&
                    track.getJSONObject("added_by").isNull("id"))
                track_add_by=track.getJSONObject("added_by").getString("id");
            //System.out.println("3. track_add_by="+track_add_by);
            String track_uri = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("uri"))
                track_uri = track.getJSONObject("track").getString("uri");
            //System.out.println("4. track_uri="+track_uri);
            String track_name = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("name"))
                track_name = track.getJSONObject("track").getString("name");
            track_name=track_name.replaceAll("\"","'").replace('\\',' ');
            //System.out.println("5. track_name="+track_name);

            String isrc = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("external_ids")
                    && !track.getJSONObject("track").getJSONObject("external_ids").isNull("isrc"))
                isrc = track.getJSONObject("track").getJSONObject("external_ids").getString("isrc");
            //System.out.println("6. isrc="+isrc);

            String artist_uri = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("artists")
                    && !track.getJSONObject("track").getJSONArray("artists").isNull(0)
                    && !track.getJSONObject("track").getJSONArray("artists").getJSONObject(0).isNull("uri"))
                artist_uri = track.getJSONObject("track").getJSONArray("artists").getJSONObject(0).getString("uri");
            //System.out.println("7. artist_uri="+artist_uri);
            String artist_name = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("artists")
                    && !track.getJSONObject("track").getJSONArray("artists").isNull(0)
                    && !track.getJSONObject("track").getJSONArray("artists").getJSONObject(0).isNull("name"))
                artist_name = track.getJSONObject("track").getJSONArray("artists").getJSONObject(0).getString("name");
            artist_name = artist_name.replaceAll("\"","'").replace('\\',' ');
            //System.out.println("8. artist_name="+artist_name);

            String album_uri = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("album")
                    && !track.getJSONObject("track").getJSONObject("album").isNull("uri"))
                album_uri = track.getJSONObject("track").getJSONObject("album").getString("uri");
            //System.out.println("9. album_uri="+album_uri);
            String album_name = "None";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("album")
                    && !track.getJSONObject("track").getJSONObject("album").isNull("name"))
                album_name = track.getJSONObject("track").getJSONObject("album").getString("name");
            album_name=album_name.replaceAll("\"","'").replace('\\',' ');
            //System.out.println("10. album_name="+album_name);

            String position = "0";
            if(!track.isNull("track")
                    && !track.getJSONObject("track").isNull("track_number")) {
                position = track.getJSONObject("track").get("track_number").toString();
                position = ""+(i+1);
            }
            //System.out.println("11. position="+position);
            String load_timestamp = "";
            if(playlist_date.length()>10)
                load_timestamp = playlist_date;
            else
                load_timestamp = playlist_date+" 00:00:00";

            if(!track.isNull("added_at"))
                load_timestamp = track.getString("added_at");
            //System.out.println("12. load_timestamp="+load_timestamp);

            StringBuffer sb = new StringBuffer();
            sb.append(playlist_uri);   //1,13
            sb.append("\",\""+playlist_date);  //2,18
            sb.append("\",\""+playlist_id);    //13 (14)
            sb.append("\",\""+playlist_owner); //14 (15)
            sb.append("\",\""+playlist_owner_uri); //15 (16)
            sb.append("\",\""+playlist_name);  //16 (17)
            sb.append("\",\""+followers);      //17 (19)
            sb.append("\",\""+isrc);           //6

            sb.append("\",\""+track_add_by);   //3
            sb.append("\",\""+track_uri);      //4
            sb.append("\",\""+track_name);     //5
            sb.append("\",\""+artist_uri);     //7
            sb.append("\",\""+artist_name);    //8
            sb.append("\",\""+album_uri);      //9
            sb.append("\",\""+album_name);     //10
            sb.append("\",\""+position);       //11
            sb.append("\",\""+load_timestamp); //12

            String record="\""+sb.toString()+"\"";
            ar.add(record);
            //if(i==0)
            //    System.out.println(record);
        }
        System.out.println("  Lines added to output="+tracks.length());
        return ar;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String today_now = ISODateTimeFormat.dateTime().print(new DateTime(org.joda.time.DateTimeZone.UTC));
        TableRow line = c.element();
        String playlist_uri = line.get("playlist_uri").toString();
        if (playlist_uri != null && playlist_uri.length()>0) {
            //spotify:user:spotify:playlist:37i9dQZF1DWY4lFlS4Pnso
            System.out.print("Playlist_URI="+playlist_uri);
            ReadHTTP rhp = new ReadHTTP();
            String playlist_id = playlist_uri.split(":")[4];
            String user_id = playlist_uri.split(":")[2];
            int counter=0;
            String response="";
            while(true) {
                try {
                    response = rhp.getBody("https://api.spotify.com/v1/users/"+user_id+"/playlists/" + playlist_id);
                    if (response.contains("\"status\" : 404")) {
                        System.out.println("      THE PLAYLIST NOT FOUND IN SPOTIFY API!!! ");
                        c.output("");
                        return;
                    }

                    if (response.contains("\"status\" : 500")) {
                        System.out.println("      SERVER ERROR IN SPOTIFY API!!! ");
                        c.output("");
                        return;
                    }
                    if(response.length()==0) {
                        System.out.println("      EMPY RESPONSE FROM SPOTIFY API!!! ");
                        c.output("");
                        return;
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    counter++;
                    if (counter > 5) {
                        c.output("");
                        return;
                    }
                }
            }

            ArrayList<String> tracks = getTrackRecords(response);
            for (int i = 0; i < tracks.size(); i++) {
                if(tracks.get(i)!=null) {
                    c.output(tracks.get(i));
                }
            }
        }
    }
}
