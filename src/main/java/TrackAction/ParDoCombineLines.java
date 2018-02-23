package TrackAction;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class ParDoCombineLines implements SerializableFunction<Iterable<String>, String> {
    String dividor="&&&";
    @Override
    public String apply(Iterable<String> input) {
        int size=0;
        StringBuffer sb = new StringBuffer();
        for(String item: input) {
            sb.append(item+dividor);
            //System.out.println(item);
        }
        String out = sb.toString();
        return out;
    }
}
