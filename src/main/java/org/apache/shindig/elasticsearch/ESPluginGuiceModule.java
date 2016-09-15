package org.apache.shindig.elasticsearch;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.crawling.ShindigCrawler;
import org.apache.shindig.elasticsearch.listeners.ElasticsearchListener;
import org.apache.shindig.elasticsearch.util.ESBulkingConnector;
import org.apache.shindig.elasticsearch.util.ESConnector;
import org.apache.shindig.elasticsearch.util.ESMappingLoader;
import org.apache.shindig.elasticsearch.util.IESConnector;

import com.google.inject.AbstractModule;

/**
 * Guice model binding the elasticsearch indexing event listener and crawler
 * into Shindig's systems.
 */
public class ESPluginGuiceModule extends AbstractModule
{
    private static final String SUBSYSTEM_NAME = "Shindig Elasticsearch Plugin";
    
    private static final String ES_CONN_PROP = "shindig.elasticsearch.connector";
    private static final String LOAD_MAPPING_PROP =
        "shindig.elasticsearch.mapping.load";

    @Override
    protected void configure()
    {
        final Logger logger = Logger.getLogger(SUBSYSTEM_NAME);
        
        try
        {
            //configuration
            final ESConfig esConfig = new ESConfig();
            bind(ESConfig.class).toInstance(esConfig);
            
            //elasticsearch connector
            String esConnProp = esConfig.getProperty(ES_CONN_PROP);
            IESConnector esConn = null;
            if("bulking".equals(esConnProp))
            {
                esConn = new ESBulkingConnector(esConfig);
            }
            else
            {
                //TODO: http?
                esConn = new ESConnector(esConfig);
            }
            bind(IESConnector.class).toInstance(esConn);
            
            ESMappingLoader mapLoader = new ESMappingLoader(esConfig, esConn);
            bind(ESMappingLoader.class).toInstance(mapLoader);

            //load mappings if configured
            boolean loadMapping = Boolean.parseBoolean(
                esConfig.getProperty(LOAD_MAPPING_PROP));
            if(loadMapping)
            {
                mapLoader.loadMappings();
            }
            
            //event-based ES indexing (self-registering)
            bind(ElasticsearchListener.class).asEagerSingleton();

            //start scheduled crawler
            bind(ShindigCrawler.class).asEagerSingleton();
        }
        catch(Exception e)
        {
            String message = e.getMessage() + '\n';
            for (final StackTraceElement ste : e.getStackTrace())
            {
                message += ste.toString() + '\n';
            }
            logger.log(Level.SEVERE, message);
            e.printStackTrace();
        }
    }

}
