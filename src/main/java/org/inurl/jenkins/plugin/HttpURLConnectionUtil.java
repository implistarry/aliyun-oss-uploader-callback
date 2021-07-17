package org.inurl.jenkins.plugin;

import net.sf.json.JSON;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class HttpURLConnectionUtil {
    /**
     * get请求
     *
     * @param path
     * @param param
     * @return
     */
    public static String get(String path,Map<String, Object> param) {
        try {
            if (param != null) {
                StringBuffer paramBuffer = new StringBuffer();
                int i = 0;
                for (String key : param.keySet()) {
                    if (i == 0)
                        paramBuffer.append("?");
                    else
                        paramBuffer.append("&");
                    paramBuffer.append(key).append("=").append(param.get(key));
                    i++;
                }
                path += paramBuffer;
            }
            URL url = new URL(path);    // 把字符串转换为URL请求地址
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();// 打开连接
            connection.connect();// 连接会话
            // 获取输入流
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {// 循环读取流
                sb.append(line);
            }
            br.close();// 关闭流
            connection.disconnect();// 断开连接
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("失败!");
            return null;
        }
    }
    /**
     * post    请求
     *
     * @param path
     * @param jsonStr
     * @return
     */
    public static String post(String path,String jsonStr) {
        try {
            URL url =  new URL(path);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("POST");// 提交模式
            httpURLConnection.setConnectTimeout(10000);//连接超时 单位毫秒
            httpURLConnection.setReadTimeout(10000);//读取超时 单位毫秒
            // 发送POST请求必须设置如下两行
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setDoInput(true);
            httpURLConnection.setRequestProperty("Charset", "UTF-8");
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

//			PrintWriter printWriter = new PrintWriter(httpURLConnection.getOutputStream());
//			printWriter.write(postContent);
//			printWriter.flush();

            httpURLConnection.connect();
            OutputStream os=httpURLConnection.getOutputStream();
            os.write(jsonStr.getBytes("UTF-8"));
            os.flush();

            StringBuilder sb = new StringBuilder();
            int httpRspCode = httpURLConnection.getResponseCode();
            if (httpRspCode == HttpURLConnection.HTTP_OK) {
                // 开始获取数据
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(httpURLConnection.getInputStream(), "utf-8"));
                String line = null;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                br.close();
                return sb.toString();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {

        Map<String,Object> map=new HashMap<>();
        map.put("A","a");
        map.put("B",100);
        String getResult = get("http://192.xxx.xx.xx:8665/testGet", map);
        System.out.println(getResult);
//        String postResult = post("http://192.xxx.xx.xx:8665/testPost", JSON.toJSONString(map));
//        System.out.println(postResult);

    }
}
