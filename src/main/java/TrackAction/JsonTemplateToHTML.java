package TrackAction;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class JsonTemplateToHTML {
    String userProlog="";
    String userEpilog="";
    String artistProlog="";
    String artistEpilog="";
    String trackProlog="";
    String trackEpilog="";
    String playlistBody="";

    String artistName="";
    String artist_uri="";
    String reportdate="";

    public JsonTemplateToHTML() throws Exception {
        ArrayList<String> ar = readTemplateFile();
        userProlog="";
        userEpilog="";
        artistProlog="";
        artistEpilog="";
        trackProlog="";
        trackEpilog="";
        playlistBody="";
        processHTML(0,ar,"user");

    }

    public ArrayList<String> readTemplateFile() throws Exception {
        ArrayList<String> ar = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        InputStream is = CreateTablePartition.class.getClassLoader().getResourceAsStream("playlist-add-drop-template.html");
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        try {
            String line = reader.readLine();
            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                ar.add(line);
                line = reader.readLine();
            }
            String everything = sb.toString();
        } finally {
            reader.close();
        }
        return ar;
    }

    private int processHTML(int index, ArrayList<String> ar, String cmd) {
        //System.out.println("AAAAAAAAA");
        for(int i=index;i<ar.size();i++) {
            if(ar.get(i).contains("<<<END OF PLAYLIST")
                    || ar.get(i).contains("<<<END OF TRACK")
                    || ar.get(i).contains("<<<END OF ARTIST")
                    ) {
                return i+1;
            }

            if(cmd.equals("user") && !ar.get(i).startsWith("<<<")) {
                userProlog  += ar.get(i)+"\n";
            }
            if(ar.get(i).contains("<<<ARTIST")) {
                i=processHTML(i+1,ar,"artist");
                cmd="end of user";
            }
            if(cmd.equals("end of user")) {
                userEpilog += ar.get(i)+"\n";
            }

            if(cmd.equals("artist") && !ar.get(i).contains("<<<")) {
                artistProlog  += ar.get(i)+"\n";
            }
            if(ar.get(i).contains("<<<TRACK")) {
                i = processHTML(i+1,ar,"track");
                cmd="end of artist";
            }
            if(cmd.equals("end of artist")) {
                artistEpilog += ar.get(i)+"\n";
            }

            if(cmd.equals("track") && !ar.get(i).contains("<<<")) {
                trackProlog  += ar.get(i)+"\n";
            }
            if(ar.get(i).contains("<<<PLAYLIST")) {
                i = processHTML(i+1,ar,"playlist");
                cmd="end of track";
            }
            if(cmd.equals("end of track")) {
                trackEpilog += ar.get(i)+"\n";
            }

            if(cmd.equals("playlist") && !ar.get(i).contains("<<<")) {
                playlistBody  += ar.get(i)+"\n";
            }
        }
        return 0;
    }

    private String getUserProlog(JSONObject user) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        String date2 = dateFormat.format(date);
        return userProlog.replace("{{ name }}",user.getString("firstname")).replace("{{ currentDate }}",date2);
    }

    private String getArtistProlog(JSONObject artist) {
        artistName=artist.getString("artist_name");
        artist_uri = artist.getString("artist_uri");
        return artistProlog
                .replace("{{ artistName }}",artist.getString("artist_name"))
                .replace("{{ artist_uri }}",artist.getString("artist_uri").split(":")[2])
                .replace("{{ artist_image }}",artist.getString("artist_image"));
    }

    private String getTrackProlog(JSONObject track) {
        return trackProlog.replace("{{ trackName }}",track.getString("product_title"))
                .replace("{{ artistName }}",artistName)
                .replace("{{ artist_uri }}",artist_uri.split(":")[2])
                .replace("{{ reportdate }}",reportdate)
                .replace("{{ track_image }}",track.getString("track_image"))
                .replace("{{ track_uri }}",track.getString("track_uri").split(":")[2]);
    }

    private String getPlaylistBody(JSONObject playlist) {
        String body = playlistBody
                .replace("{{ action }}",playlist.getString("action_type"))
                .replace("{{ playlistName }}",playlist.getString("name"))
                .replace("{{ playlist_uri }}",playlist.getString("playlist_uri").split(":")[4])
                .replace("{{ position }}",playlist.getString("position"))
                .replace("{{ playlistOwner }}",playlist.getString("owner_id"))
                .replace("{{ playlistCountry }}",playlist.getString("country"))
                .replace("{{ followers }}",playlist.getString("followers"));
                //.replace("{{ currentStreams }}",playlist.getString("streams"))
                //.replace("{{ estStreams }}",playlist.getString("estimated_streams"));

        if(playlist.getString("action_type").equals("DROP")) {
            //body = body.replace("action-add", "action-drop");
            body = body.replace("{{ background-color }}","background-color: #F75A52;");
        } else {
            body = body.replace("{{ background-color }}","background-color: #58D765;");
        }
        return body;
    }

    private String processUser(JSONObject user){
        StringBuffer sb = new StringBuffer();
        sb.append(getUserProlog(user));
        JSONArray artists = user.getJSONArray("artists");
        for(int i = 0; i< artists.length();i++) {
            JSONObject artist=artists.getJSONObject(i);
            sb.append(processArtist(artist));
        }
        sb.append(userEpilog);
        return sb.toString();
    }
    private String processArtist(JSONObject artist) {
        StringBuffer sb = new StringBuffer();
        sb.append(getArtistProlog(artist));
        JSONArray tracks = artist.getJSONArray("tracks");
        for(int i = 0; i< tracks.length();i++) {
            JSONObject track=tracks.getJSONObject(i);
            sb.append(processTrack(track));
        }
        sb.append(artistEpilog);
        return sb.toString();
    }

    private String processTrack(JSONObject track) {
        StringBuffer sb = new StringBuffer();
        sb.append(getTrackProlog(track));
        JSONArray playlists = track.getJSONArray("playlists");
        for(int i = 0; i< playlists.length();i++) {
            JSONObject playlist=playlists.getJSONObject(i);
            sb.append(processPlaylist(playlist));
        }
        sb.append(trackEpilog);
        return sb.toString();
    }

    private String processPlaylist(JSONObject playlist) {
        StringBuffer sb = new StringBuffer();
        sb.append(getPlaylistBody(playlist));
        //sb.append(trackEpilog);
        return sb.toString();
    }

    public String processJson(JSONObject user, String reportdate) {
        this.reportdate = reportdate;
        String result = processUser(user);
        return result;
    }

    public String getJSONasString() throws Exception{
        File file = new File("resources/email.json");
        Scanner sc = new Scanner(file);
        sc.useDelimiter("\\Z");
        String json = sc.next();
        return json;
    }

    public void saveHTML(String filename,String body) {
        try{
            PrintWriter writer = new PrintWriter(filename, "UTF-8");
            writer.println(body);
            writer.close();
        } catch (IOException e) {
            // do something
        }
    }

    public static void main(String[] args) throws Exception{
        JsonTemplateToHTML jth = new JsonTemplateToHTML();
        System.out.print("upppp1="+jth.userProlog);
        System.out.print("apppp2="+jth.artistProlog);
        System.out.print("tpppp3="+jth.trackProlog);
        System.out.print("pbppp4="+jth.playlistBody);
        System.out.print("teppp5="+jth.trackEpilog);
        System.out.print("aeppp6="+jth.artistEpilog);
        System.out.print("ueppp7="+jth.userEpilog);
        String result = jth.processJson(new JSONObject(jth.getJSONasString()),"2017-08-04");
        System.out.println(result);
        try{
            PrintWriter writer = new PrintWriter("resources/email_result.html", "UTF-8");
            writer.println(result);
            writer.close();
        } catch (IOException e) {
            // do something
        }
        System.out.println("Process Json");
    }
}
