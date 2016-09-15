package org.apache.shindig.elasticsearch.util;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.google.inject.Injector;

/**
 * Servler status listener safely terminating the connection to Elasticsearch
 * while the classloader is still active to prevent unclean Shutdowns etc..
 */
public class ConnectorCleanupListener implements ServletContextListener
{
    public static final String INJECTOR_ATTRIBUTE = "guice-injector";
    
    @Override
    public void contextInitialized(ServletContextEvent sce)
    {
        //nop
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce)
    {
        ServletContext context = sce.getServletContext();
        Injector injector = (Injector) context.getAttribute(INJECTOR_ATTRIBUTE);
        
        IESConnector conn = injector.getInstance(IESConnector.class);
        if(conn != null)
        {
            conn.close();
        }
    }
}
