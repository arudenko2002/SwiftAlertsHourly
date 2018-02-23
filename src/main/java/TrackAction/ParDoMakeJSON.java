package TrackAction;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;

public class ParDoMakeJSON extends DoFn<KV<String,Iterable<TableRow>>,String> {

    private String formatInteger(String number) {
        DecimalFormat df2 = new DecimalFormat( "#,###,###,##0" );
        try {
            int i = Integer.parseInt(number);
            return df2.format(i);
        } catch(Exception e) {
            System.out.println("I cannot parse 'followers'!!!");
            e.printStackTrace();
            return "0,000";
        }
    }

    private JSONObject getPlaylistJson(JSONObject record) {
        JSONObject playlist = new JSONObject();
        playlist.put("name",record.get("name"));
        playlist.put("playlist_uri",record.get("playlist_uri"));
        playlist.put("owner_id",record.get("owner_id"));
        playlist.put("country",record.get("country"));
        String followers_formatted = formatInteger(record.get("followers").toString());
        playlist.put("followers",followers_formatted);
        playlist.put("position",record.get("position"));
        playlist.put("action_type",record.get("action_type"));

        //playlist.put("streams",record.get("streams"));
        //playlist.put("estimated_streams",record.get("estimated_streams"));
        return playlist;
    }

    private JSONObject getTrackJson(JSONObject record) throws Exception{
        JSONObject track = new JSONObject();
        /*
        if(record.isNull("track_image")) {
            SSLEmail ssle = new SSLEmail();
            String body="The track image is empty.  The previous step failed in BigQuery, restart it.  Exiting now.."
                    +"\n"+record.get("track_uri")+" "+record.get("isrc");
            System.out.println(body);
            ssle.sendMail("alexey.rudenko@umusic.com", body, body, "justtome");
            //System.exit(-1);
            track.put("track_image","track_image is missed, check your mail!");
        } else {
            track.put("track_image",record.get("track_image"));
        }*/
        track.put("product_title",record.get("product_title"));
        track.put("isrc",record.get("isrc"));
        track.put("track_uri",record.get("track_uri"));

        return track;
    }

    private JSONObject getArtistJson(JSONObject record) throws Exception{
        JSONObject artist = new JSONObject();
        artist.put("artist_name",record.get("artist_name"));
        /*
        if(record.isNull("artist_image")) {
            SSLEmail ssle = new SSLEmail();
            String body="The artist image is empty.  The previous step failed in BigQuery, restart it.  Exiting now..";
            body+="\n"+record.get("track_uri")+" "+record.get("isrc");
            System.out.println(body);
            ssle.sendMail("alexey.rudenko@umusic.com", body, body, "justtome");
            //System.exit(-1);
            artist.put("artist_image","artist_image is missed, check your mail!");
        } else {
            artist.put("artist_image",record.get("artist_image"));
        }*/
        artist.put("artist_uri",record.get("artist_uri"));
        return artist;
    }

    private JSONObject getUserJson(JSONObject record, String actualDate) {
        JSONObject user = new JSONObject();
        user.put("firstname",record.get("firstname"));
        user.put("email",record.get("email"));
        user.put("reportdate",actualDate);
        return user;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        String f_email = "";
        String f_artist_name = "";
        String f_isrc = "";
        JSONArray playlists = new JSONArray();
        JSONObject track = new JSONObject();
        JSONArray tracks = new JSONArray();
        JSONObject artist = new JSONObject();
        JSONArray artists = new JSONArray();
        JSONObject user = new JSONObject();
        playlists = new JSONArray();

        KV<String, Iterable<TableRow>> pair = c.element();
        Iterator<TableRow> rows = pair.getValue().iterator();
        while(rows.hasNext()) {
            TableRow tr = rows.next();
            JSONObject record = new JSONObject(tr);
            //HashMap<String,String> record = getRecord(row);
            String o_email=record.getString("email").replace("\n","");
            String o_artist_name = record.getString("artist_name").replace("\n","");
            String o_isrc = record.getString("isrc").replace("\n","");
            String actualDate=record.getString("reportdate").replace("\n","");
            JSONObject playlist = getPlaylistJson(record);
            if(f_email.length()>0 && f_email.equals(o_email) && f_artist_name.equals(o_artist_name) && f_isrc.equals(o_isrc)) {
                playlists.put(playlist);
            } else if(f_email.length()>0 && f_email.equals(o_email) && f_artist_name.equals(o_artist_name) && !f_isrc.equals(o_isrc)) {
                track.put("playlists",playlists);
                tracks.put(track);

                //Initialize, start new track
                track = getTrackJson(record);
                playlists = new JSONArray();
                playlists.put(playlist);
            } else if(f_email.length()>0 && f_email.equals(o_email) && !f_artist_name.equals(o_artist_name)) {
                track.put("playlists",playlists);
                tracks.put(track);
                artist.put("tracks",tracks);
                artists.put(artist);

                //Initialize new artist
                playlists = new JSONArray();
                playlists.put(playlist);
                tracks = new JSONArray();
                track = getTrackJson(record);
                artist = getArtistJson(record);
            } else if(f_email.length()==0 && o_email.length()>0) {
                //Initial record
                playlists = new JSONArray();
                playlists.put(playlist);
                track = getTrackJson(record);
                tracks = new JSONArray();
                artist = getArtistJson(record);
                artists = new JSONArray();
                user = getUserJson(record,actualDate);
            }
            f_email=record.getString("email").replace("\n","");
            f_artist_name = record.getString("artist_name").replace("\n","");
            f_isrc = record.getString("isrc").replace("\n","");
        }
        track.put("playlists",playlists);
        tracks.put(track);
        artist.put("tracks",tracks);
        artists.put(artist);
        user.put("artists",artists);
        String json=user.toString();
        System.out.println("json="+json);
        c.output(json);
    }
}