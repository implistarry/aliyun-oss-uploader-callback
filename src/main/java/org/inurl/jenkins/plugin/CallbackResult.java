package org.inurl.jenkins.plugin;

import net.sf.json.JSONObject;

public class CallbackResult {
    public boolean success;
    public String msg;
    public String path;
    public String gameName;
    public String gameId;
    public int remainingTaskNum;
    public String channelLabel;

    public String getJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.element("success",success);
        jsonObject.element("msg",msg);
        jsonObject.element("path",path);
        jsonObject.element("gameName",gameName);
        jsonObject.element("gameId",gameId);
        jsonObject.element("channelLabel",channelLabel);
        jsonObject.element("remainingTaskNum",remainingTaskNum);
        return jsonObject.toString();
    }
}
