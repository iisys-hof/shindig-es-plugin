package org.apache.shindig.elasticsearch.util;

import java.net.URL;
import java.util.List;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.ESConfig;
import org.json.JSONObject;

/**
 * Deprecated elasticsearch connector using the ES REST API.
 */
public class ESConnectorHttp
{
    private static final String URL_PROP = "shindig.elasticsearch.url";
    
    private final String fEsUrl;
    
    private final Logger fLogger;
    
    public ESConnectorHttp(ESConfig config)
    {
        fEsUrl = config.getProperty(URL_PROP);
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
    }
    
    public void add(String index, String type, String id, JSONObject entry)
        throws Exception
    {
        final String pushUrl = fEsUrl + index + "/" + type + "/";
        
        //send data
        String content = entry.toString();
        
        URL url = new URL(pushUrl + entry.getString("id"));
        
        //TODO: check if update is even required ?
        
        //TODO: put or post? - update or new?
        //send
        //System.out.println("\nsending:\n" + content);
        String response = HttpUtil.sendJson(url, "PUT", content);
        
        //TODO: log answer?
//        fLogger.log(Level.WARNING, "entry added, response:\n" + response);
    }
    
    public void update(String index, String type, String id, JSONObject entry)
        throws Exception
    {
        final String pushUrl = fEsUrl + index + "/" + type + "/";
        
        //send data
        String content = entry.toString();
        
        URL url = new URL(pushUrl + entry.getString("id"));
        
        //TODO: check if update is even required ?
        
        //TODO: put or post? - update or new?
        //send
        //System.out.println("\nsending:\n" + content);
        String response = HttpUtil.sendJson(url, "PUT", content);
        
        //TODO: log answer?
//        fLogger.log(Level.WARNING, "entry updated, response:\n" + response);
    }
    
    public void delete(String index, String type, String id) throws Exception
    {
        
    }
    
    public List<JSONObject> getAll(String index, String type) throws Exception
    {
        return null;
    }
    
    public JSONObject get(String index, String type, String id) throws Exception
    {
        return null;
    }
}
