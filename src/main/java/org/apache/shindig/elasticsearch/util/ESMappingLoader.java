package org.apache.shindig.elasticsearch.util;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.ESConfig;
import org.json.JSONObject;

/**
 * Utility that loads a mapping file from the classpath and sets the
 * appropriate per-type mappings in Elasticsearch.
 * Currently reads the file "mapping.json".
 */
public class ESMappingLoader
{
    private static final String INDEX_NAME = "shindig.elasticsearch.index";
    private static final String TYPES_PROP = "shindig.elasticsearch.mapping.load.types";
    
    private final IESConnector fConn;
    
    private final String fIndex, fTypesString;
    
    private final Logger fLogger;
    
    /**
     * Creates a mapping loader with the given configuration, sending mappings
     * via the given elasticsearch connector.
     * None of the parameters may be null.
     * 
     * @param config configuration object to use
     * @param connector elasticsearch connector to use
     */
    public ESMappingLoader(ESConfig config, IESConnector connector)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(connector == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        
        fConn = connector;
        
        fIndex = config.getProperty(INDEX_NAME);
        fTypesString = config.getProperty(TYPES_PROP);
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
    }
    
    /**
     * Loads the configured mappings from the classpath and sets them in
     * elasticsearch.
     */
    public void loadMappings()
    {
        try
        {
            fLogger.log(Level.INFO,
                "loading mappings for: " + fTypesString);
            
            String[] types = fTypesString.split(",");
            
            //retrieve mapping from resources
            JSONObject mappings = getMappings();
            
            //load all specified mappings
            for(String type : types)
            {
                JSONObject m = mappings.optJSONObject(type);
                if(m != null)
                {
                    fConn.setMapping(fIndex, type, m);
                }
                else
                {
                    fLogger.log(Level.WARNING,
                        "failed to load mapping for: " + type);
                }
            }
        }
        catch(Exception e)
        {
            fLogger.log(Level.SEVERE, "failed to load mappings", e);
        }
    }
    
    private JSONObject getMappings() throws Exception
    {
        //read from classpath
        InputStream is = this.getClass().getResourceAsStream("/mapping.json");
        
        //read into buffers
        List<byte[]> buffers = new LinkedList<byte[]>();
        byte[] buffer = new byte[1024];
        int read = is.read(buffer);
        int total = 0;
        while(read > 0)
        {
            total += read;
            buffers.add(Arrays.copyOf(buffer, read));
            
            read = is.read(buffer);
        }
        
        //copy to usable array
        byte[] file = new byte[total];
        total = 0;
        for(byte[] b : buffers)
        {
            System.arraycopy(b, 0, file, total, b.length);
            total += b.length;
        }
        
        //convert to String and JSON
        String mappingString = new String(file, Charset.forName("UTF-8"));
        JSONObject mappings = new JSONObject(mappingString);
        
        return mappings;
    }
}
