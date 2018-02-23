package TrackAction;


import org.apache.beam.sdk.transforms.SerializableFunction;

public class JsonListOutput implements SerializableFunction<Iterable<String>, String> {

        @Override
        public String apply(Iterable<String> lines) {
            //System.out.println("line11="+line);
            StringBuffer sb = new StringBuffer();
            for(String line: lines) {
                if(sb.length()>0 && line.length()>0)
                    sb.append(",");
                sb.append(line);
            }
            String result=sb.toString();
            if(!result.startsWith("["))
                result = "["+result+"]";
            return result;
        }
    }


