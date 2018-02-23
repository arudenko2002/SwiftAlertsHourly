package TrackAction;

import okhttp3.*;
import okhttp3.OkHttpClient.Builder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ReadHTTP {
    static String auth = "Bearer BQA-8Tc-ktLSjuJc13M-4MqYt3FJE2SFCKH-QVA4N48IYkg08eEnUWgFzAdEnPN8lPsbubOfsC_FbZu6Vo7qoA";

    String track_image = "";
    String artist_image = "";
    String artist_uri = "";
    String url="https://api.spotify.com/v1/tracks/";

    public String getBody(String href) throws Exception {
        String body = "";
        int counter=0;
        while(true) {
            counter++;
            if(counter>10) {
                System.out.println("The call was done "+counter+" times without success. exiting...");
                return "";
            }
            OkHttpClient client = new OkHttpClient.Builder()
                    .authenticator(new Authenticator() {
                        @Override
                        public Request authenticate(Route route, Response response) throws IOException {
                            return response.request().newBuilder()
                                    .header("Authorization", auth)
                                    .build();
                        }
                    })
                    .followRedirects(true)
                    .followSslRedirects(true)
                    .build();
            //System.out.println(client);

            Request request = new Request.Builder()
                    .url(href)
                    .get()
                    .addHeader("content-type", "application/json; charset=UTF-8")
                    .addHeader("authorization", auth)
                    .build();
            //System.out.println(request);
            try {
                //System.out.println("API request to spotify");
                Response response = client.newCall(request).execute();
                body = response.body().string();
                //Check the response (can be "503, Service" umavailable reponse)
                JSONObject b = new JSONObject(body);
                Thread.sleep(2 * 1000);
                break;
            } catch (InterruptedException e) {
                System.out.println("Interrupted Exception!!!");
                e.printStackTrace();
            } catch (java.net.ProtocolException e) {
                synchronized(this) {
                    RefreshToken rt = new RefreshToken();
                    auth = "Bearer " + rt.refreshToken();
                    System.out.println("_TOKEN=" + auth);
                }
            } catch (java.net.SocketTimeoutException e) {
                System.out.println("Socket Timeout!!!");
                e.printStackTrace();
            } catch (org.json.JSONException e) {
                System.out.println("Not Json? Service is unavailable, 503?");
                e.printStackTrace();
                System.out.println("Waiting for 5 seconds...");
                Thread.sleep(5 * 1000);
            }
        }
        return body;
    }

    private String getTrackImageBody(String body) {
        int resolution=2; //The last one in the list of 3, the smallest
        int TRACKS=1;
        JSONObject b = new JSONObject(body);
        if(!b.isNull("album") &&!b.getJSONObject("album").isNull("images")&&!b.getJSONObject("album").getJSONArray("images").isNull(2)) {
            JSONArray images = b.getJSONObject("album").getJSONArray("images");
            if(images.isNull(0)) {
                System.out.println("No artist image found!");
                return "various artists";
            } else {
                int imagesn=images.length();
                if(imagesn>1) {
                    resolution=imagesn-TRACKS;
                } else {
                    resolution=0;
                }
                JSONObject image = images.getJSONObject(resolution);
                return image.getString("url");
            }

        }else {
            return "no track/album image uri";
        }
    }

    private String getArtistHref(String body,String artist_name) {
        JSONObject b = new JSONObject(body);
        if(!b.isNull("artists")&& !b.getJSONArray("artists").isNull(0)) {
            JSONArray artists = b.getJSONArray("artists");
            int artist_index=getArtistIndex(artists,artist_name);
            JSONObject href = artists.getJSONObject(artist_index);
            System.out.println("Found Artist=="+artist_index+"    "+href.getString("name"));
            System.out.println("From CANOPUS=="+artist_name);
            return href.getString("href");
        } else {
            return "no artist image uri";
        }
    }

    private int getArtistIndex(JSONArray artists,String artist_name) {
        int result=0;
        if(artist_name.length()==0)return result;

        double similarity=0.0;
        for(int i=0; i<artists.length();i++) {
            JSONObject href = artists.getJSONObject(i);
            String name=href.getString("name");
            if(artist_name.equals("Töpper, Hertha")) {
                int hh=8;
            }
            double sim = getSimilarity(artist_name, name);
            System.out.println("canopus artist_name="+artist_name+"  list artist name="+name+"   sim="+sim);
            if(similarity<sim) {
                similarity=sim;
                result=i;
            }
        }
        System.out.println("SIMILARITY="+similarity);
        return result;
    }

    private double getSimilarity(String n1, String n2) {
        System.out.println("n1="+n1+" n2="+n2);
        if(n1.contains(",")) {
            String[] n11=n1.split(",");
            if(n11.length>1)
                n1=n11[1].trim()+" "+n11[0].trim();
            else
                n1=n11[0].trim();
        }
        if(n2.contains(",")) {
            String[] n22=n2.split(",");
            if(n22.length>1)
                n2=n22[1].trim()+" "+n22[0].trim();
            else
                n2=n22[0].trim();
        }
        String name1=n1.toLowerCase();
        String name2=n2.toLowerCase();
        if(name1.equals(name2)) return 1.0;
        String[] s1 = (name1.replace(".","").split(" "));
        String[] s2 = (name2.replace(".","").split(" "));
        if(s1.length==1 && s2.length==1) {
            return getDiff(s1[0],s2[0]);
        }

        if(s1.length==1 && s2.length==2) {
            if(s1[0].equals(s2[0]) || s1[0].equals(s2[1])) return 0.8;
            return Math.max(getDiff(s1[0],s2[0]),getDiff(s1[0],s2[1]));
        }
        if(s1.length==2 && s2.length==1) {
            if(s2[0].equals(s1[0]) || s1[1].equals(s2[0])) return 0.8;
            return Math.max(getDiff(s1[0],s2[0]),getDiff(s1[1],s2[0]));
        }

        if(s1.length==2 && s2.length==2) {
            if(s2[0].equals(s1[0]) && s1[1].equals(s2[1])) return 1.0;
            System.out.println(s1[0]+"   "+s2[0]);
            System.out.println(getDiff(s1[0],s2[0]));
            System.out.println(getDiff(s1[1],s2[1]));
            return getDiff(s1[0],s2[0])*getDiff(s1[1],s2[1]);
        }

        if(s1.length==2 && s2.length==3) {
            if(s1[0].equals(s2[0]) && s1[1].equals(s2[2])) return 0.9;
            return getDiff(s1[0],s2[0])*getDiff(s1[1],s2[2]);
        }
        if(s1.length==3 && s2.length==2) {
            if(s2[0].equals(s1[0]) && s1[2].equals(s2[1])) return 0.9;
            return getDiff(s1[0],s2[0])*getDiff(s1[2],s2[1]);
        }

        if(s1.length==3 && s2.length==3) {
            if(s2[0].equals(s1[0]) && s1[2].equals(s2[2])) return 1.0;
            return getDiff(s1[0],s2[0])*getDiff(s1[2],s2[2]);
        }

        return 0.0;
    }

    private char[] sort(String s) {
        char tempArray[] = s.toCharArray();
        Arrays.sort(tempArray);
        return tempArray;
    }

    private double getDiff(String s1, String s2) {
        if(s1.length()==0 && s2.length()==0)return 1.0;
        if(s1.length()==0 || s2.length()==0)return 0.0;
        //System.out.println("1="+s1);
        //System.out.println("2="+s2);
        char[] ss1 = sort(s1);
        char[] ss2 = sort(s2);
        int och=0;
        int j=0;
        int i=0;
        while(true) {
            //System.out.println("i="+i+" ss1[i]="+ss1[i]+"   j="+j+" ss2="+ss2[j]);
            if(i==8) {
                int k=0;
            }
            if(ss1[i]==ss2[j]) {
                och++;
                i++;
                j++;
                if(ss1.length==i || ss2.length==j) break;
                continue;
            }
            if(ss1[i]>ss2[j]) {
                j++;
                if(ss2.length==j) break;
            }
            if(ss1[i]<ss2[j]) {
                i++;
                if(ss1.length==i) break;
            }
        }
        return 1.0*och/Math.max(ss1.length,ss2.length);
    }


    private String getArtistImageBody(String body) {
        int ARTISTS=1; //choose artist before the last one in the list of 3
        int resolution = 2;//the last one in the list(smallest)
        //System.out.println("Artist image body="+body);
        JSONObject b = new JSONObject(body);
        if(b.isNull("images")) {
            System.out.println("No Section IMAGES at all!!");
            System.out.println(body);
            //System.exit(200);
            return "various artists";
        }
        if(!b.isNull("uri")) {
            artist_uri = b.getString("uri");
        }
        JSONArray images = b.getJSONArray("images");

        if(images.isNull(0)) {
            System.out.println("No artist image found!");
            return "various artists";
        } else {
            int imagesn=images.length();
            if(imagesn>1) {
                resolution=imagesn-ARTISTS;
            } else {
                resolution=0;
            }
            JSONObject href = images.getJSONObject(resolution);
            return href.getString("url");
        }
    }

    public void getImages(String uri, String artist_name) throws Exception{
        // Read track data
        String track_json_body = getBody(url+uri);
        //System.out.println("URI="+url+uri);
        //System.out.println("track body="+track_json_body);
        // Get track image
        track_image = getTrackImageBody(track_json_body);
        // Get artist href from track data
        String artist_href = getArtistHref(track_json_body,artist_name);
        // Read arstist data
        if(!artist_href.startsWith("no ")) {
            String artist_json_body = getBody(artist_href);
            // Get artist image url
            artist_image = getArtistImageBody(artist_json_body);
        } else {
            System.out.println("no artist found");
        }
    }

    public void test() throws Exception {
        ArrayList<String> ar= new ArrayList<String>();
        ar.add("spotify:track:05zvi8nm6Z7AHX3wOKxcva");
        ar.add("spotify:track:0PrLodAzseOnN1EACxuAfV");
        //ar.add("spotify:track:0eK5y1c7WDyWwx6k9SFjbT");
        //ar.add("spotify:track:0qXdJdBpOdqm1x70L5VxjG");
        //ar.add("spotify:track:386i986Vk3oWiUgwRU5vV2");
        //ar.add("spotify:track:77AJ7lv6iU2weqjX6lEx4E");
        //ar.add("spotify:track:5ASZFbCod633wLragmPYTj");
        //ar.add("spotify:track:4RmU5uXxD5JzBc1wfBK6Gb");
        ReadHTTP rhp = new ReadHTTP();
        for(int i=0; i<ar.size();i++) {
            String http="https://api.spotify.com/v1/tracks/"+ar.get(i).split(":")[2];
            System.out.println(http);
            String body = rhp.getBody(http);
            System.out.println("Track body="+body);
            String href = rhp.getTrackImageBody(body);
            System.out.println("HREF track image="+href);
            String artisthref = rhp.getArtistHref(body,"");
            System.out.println("HREF artist="+artisthref);
            String ab = rhp.getBody(artisthref);
            System.out.println("Artist body="+ab);
            String aimage = rhp.getArtistImageBody(ab);
            System.out.println("HREF artist image="+aimage);
        }
        System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
        ReadHTTP rhp = new ReadHTTP();
        System.out.println(rhp.getSimilarity("Hector Zuleta","Héctor Zuleta"));
        System.exit(0);

        rhp.test();
        //rhp.getImages("568BqBOqxp0xyv93dmjv3Q");
        String body = rhp.getBody("https://api.spotify.com/v1/tracks/568BqBOqxp0xyv93dmjv3Q");
        System.out.println("AAAAAAAA="+body);
        String href = rhp.getTrackImageBody(body);
        System.out.println("GGGGGGGGGGGtrack="+href);
        //String ss  = rhp.getBody(href);
        //System.out.println("SSSSSSS==="+ss);
        String ar = rhp.getArtistHref(body,"");
        System.out.println("TTTTTTTT"+ar);
        String dd = rhp.getBody(ar);
        System.out.println(dd);
        String arrr = rhp.getArtistImageBody(dd);
        System.out.println("GGGGGGGGGGGGGartist="+arrr);
        System.out.println("\n\n\n");

    }
}
