package org.apache.shindig.elasticsearch.crawling;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.ESConfig;
import org.apache.shindig.elasticsearch.util.ESMappingLoader;
import org.apache.shindig.elasticsearch.util.IESConnector;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Runnable crawler, matching all available Shindig data to data in the
 * elasticsearch index.
 * 
 * Currently supports monthly, weekly, daily and one-time crawls, also on
 * startup.
 */
@Singleton
public class ShindigCrawler implements Runnable
{
    private static final String SHINDIG_INDEX = "shindig.elasticsearch.index";
    
    private static final String CRAWL_ON_START = "shindig.elasticsearch.startup_crawl";
    private static final String CRAWL_ENABLED = "shindig.elasticsearch.full_crawl";
    private static final String CRAWL_INTERVAL = "shindig.elasticsearch.crawl.interval";
    private static final String CRAWL_HOUR = "shindig.elasticsearch.crawl.hour";
    private static final String CRAWL_DAY = "shindig.elasticsearch.crawl.day";
    
    private static final String CLEAR_ON_START = "shindig.elasticsearch.crawl.index.clear_on_start";
    private static final String CLEAR_INTERVAL = "shindig.elasticsearch.crawl.index.clear_interval";
    
    private static final String LOAD_MAPPING_PROP =
        "shindig.elasticsearch.mapping.load";

    private static final String MONTHLY = "monthly";
    private static final String WEEKLY = "weekly";
    private static final String DAILY = "daily";
    private static final String ONCE = "once";
    
    private final IESConnector fEsConn;
    
    private final ESMappingLoader fMapLoader;
    
    private final String fIndex;
    
    private final String fInterval;
    private final int fCrawlHour, fCrawlDay;
    
    private final List<ICrawler> fCrawlers;
    
    private final Object fTrigger;
    
    private final boolean fCrawlEnabled, fCrawlOnStart, fClearOnStart,
        fLoadMapping;
    
    private final int fClearInterval;
    
    private final Calendar fCal;
    
    private long fLastCrawl;
    
    private boolean fActive;
    
    private int fClearCounter = 0;
    
    private final Logger fLogger;
    
    /**
     * Creates and automatically starts a crawler sequentially triggering the
     * sub-crawlers delivered by the given crawler factory.
     * The given configuration object, the crawler factory, the connector and
     * the mapping loader must not be null.
     * 
     * @param config configuration object to use
     * @param crawlerFact crawler factory providing sub-crawlers
     * @param conn elasticsearch connector to use
     * @param mapLoader mapping loader used if the index is cleared
     */
    @Inject
    public ShindigCrawler(ESConfig config, CrawlerFactory crawlerFact,
        IESConnector conn, ESMappingLoader mapLoader)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(crawlerFact == null)
        {
            throw new NullPointerException("crawler factory was null");
        }
        if(conn == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        if(mapLoader == null)
        {
            throw new NullPointerException("mapping loader was null");
        }
        
        fEsConn = conn;
        fMapLoader = mapLoader;
        
        fIndex = config.getProperty(SHINDIG_INDEX);
        
        fTrigger = new Object();
        fCrawlers = crawlerFact.getCrawlers();
        
        //read configuration
        fInterval = config.getProperty(CRAWL_INTERVAL);
        fCrawlHour = Integer.parseInt(config.getProperty(CRAWL_HOUR));
        fCrawlDay = Integer.parseInt(config.getProperty(CRAWL_DAY));
        
        fCrawlEnabled = Boolean.parseBoolean(config.getProperty(CRAWL_ENABLED));
        fCrawlOnStart = Boolean.parseBoolean(config.getProperty(CRAWL_ON_START));
        fClearOnStart = Boolean.parseBoolean(config.getProperty(CLEAR_ON_START));
        
        fClearInterval = Integer.parseInt(config.getProperty(CLEAR_INTERVAL));
        
        fLoadMapping = Boolean.parseBoolean(config.getProperty(LOAD_MAPPING_PROP));
        
        fCal = new GregorianCalendar();
        
        fLastCrawl = -1;
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
        
        fLogger.log(Level.INFO, "scheduler: starting shindig ES crawler");
        new Thread(this).start();
    }
    
    private void clearIndex()
    {
        fLogger.log(Level.INFO, "clearing index");
        
        try
        {
            fEsConn.clearIndex(fIndex);
            
            //load mapping again after it was cleared
            if(fLoadMapping)
            {
                fMapLoader.loadMappings();
            }
        }
        catch(Exception e)
        {
            fLogger.log(Level.SEVERE, "could not clear index", e);
        }
    }

    @Override
    public void run()
    {
        fActive = fCrawlEnabled;
        boolean crawl = fCrawlOnStart;
        
        if(fClearOnStart)
        {
            clearIndex();
        }
        
        while(fActive)
        {
            fLogger.log(Level.INFO, "scheduler: starting crawl");
            
            //"crawl" used to determine whether to crawl at startup
            if(crawl)
            {
                //clear index every n iterations if configured
                if(fClearInterval > 0
                    && ++fClearCounter % fClearInterval == 0)
                {
                    clearIndex();
                }
                
                //sequentially trigger sub-crawlers
                for(ICrawler crawler : fCrawlers)
                {
                    crawler.crawl();
                }
                
                //note time to determine next scheduled crawl
                fLastCrawl = System.currentTimeMillis();
            }
            
            //wait for next crawl
            interval();
            crawl = true;
        }
    }
    
    private void interval()
    {
        long millisecs = -1;
        
        //set calendar to last crawl time to determine next iteration
        if(fLastCrawl != -1)
        {
            fCal.setTimeInMillis(fLastCrawl);
        }
        else
        {
            fCal.setTimeInMillis(System.currentTimeMillis());
        }
        
        //clear minutes and seconds
        fCal.set(Calendar.MINUTE, 0);
        fCal.set(Calendar.SECOND, 0);

        //determine time to next crawl operation
        switch(fInterval)
        {
            //crawl once at startup, then stop
            case ONCE:
                fActive = false;
                break;
            
            //crawl daily at given hour
            case DAILY:
                //if already past that hour, skip to next day
                if(fCal.get(Calendar.HOUR_OF_DAY) >= fCrawlHour)
                {
                    fCal.add(Calendar.DAY_OF_YEAR, 1);
                }
                fCal.set(Calendar.HOUR_OF_DAY, fCrawlHour);
                break;
            
            //crawl weekly at given day of week and hour
            case WEEKLY:
                //if already past crawl this week, skip to next
                if(fCal.get(Calendar.DAY_OF_WEEK) > fCrawlDay
                    || fCal.get(Calendar.DAY_OF_WEEK) == fCrawlDay
                    && fCal.get(Calendar.HOUR_OF_DAY) >= fCrawlHour)
                {
                    fCal.add(Calendar.WEEK_OF_YEAR, 1);
                }
                fCal.set(Calendar.DAY_OF_WEEK, fCrawlDay);
                fCal.set(Calendar.HOUR_OF_DAY, fCrawlHour);
                break;

            //crawl monthly at given day of month and hour
            case MONTHLY:
                //if already past crawl this month, skip to next
                if(fCal.get(Calendar.DAY_OF_MONTH) > fCrawlDay
                    || fCal.get(Calendar.DAY_OF_MONTH) == fCrawlDay
                    && fCal.get(Calendar.HOUR_OF_DAY) >= fCrawlHour)
                {
                    fCal.add(Calendar.MONTH, 1);
                }
                fCal.set(Calendar.DAY_OF_MONTH, fCrawlDay);
                fCal.set(Calendar.HOUR_OF_DAY, fCrawlHour);
                break;
        }
        
        try
        {
            //target is set in calendar
            millisecs = fCal.getTimeInMillis();
            
            //subtract current time from target
            millisecs -= System.currentTimeMillis();
            
            if(millisecs > 0)
            {
                synchronized(fTrigger)
                {
                    fLogger.log(Level.INFO, "scheduler: waiting "
                        + millisecs + " ms until next crawl");
                    fTrigger.wait(millisecs);
                }
            }
            else
            {
                fLogger.log(Level.WARNING, "scheduler: 0 or negative wait time");
            }
        }
        catch (InterruptedException e)
        {
            fLogger.log(Level.WARNING, "scheduler interrupted", e);
        }
    }
    
    /**
     * Stops the running crawler thread.
     */
    public void stop()
    {
        fActive = false;
        synchronized(fTrigger)
        {
            fTrigger.notify();
        }
    }
}
