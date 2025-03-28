package log_split_and_Interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class My_Interceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String s = new String(event.getBody());
        try {
            JSONObject jsonObject = JSON.parseObject(s);
            String string = jsonObject.getString("ts");
            headers.put("timestamp",string);
            return event;
        } catch (Exception e) {
            return null;
        }


    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> event = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            event.add(intercept(list.get(i)));
        }
        return null;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new My_Interceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }



}
