package Comments;

import com.alibaba.fastjson.JSONObject;
import com.github.kevinsawicki.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;


public class Emotion {

    private static String server_url = "http://123.206.228.144:9960/predict";

    static int sentimentClassify(String text) {
        Map<String, String> data = new HashMap<>();
        data.put("comment", text);
        HttpRequest request = HttpRequest.post(server_url).form(data);
        String result = request.body();
        JSONObject json = (JSONObject) JSONObject.parse(result);

        switch (json.getString("emotion")) {
            case "positive": return 1;
            case "negative": return -1;
            default: return 0;
        }
    }

    public static void main(String[] args) {
        // 调用接口
        String text = "这家店太差了！太差了！";
        System.out.println(sentimentClassify(text));
    }

}
