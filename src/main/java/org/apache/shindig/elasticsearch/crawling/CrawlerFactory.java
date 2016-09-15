package org.apache.shindig.elasticsearch.crawling;

import java.util.ArrayList;
import java.util.List;

import org.apache.shindig.elasticsearch.ESConfig;
import org.apache.shindig.elasticsearch.util.IESConnector;
import org.apache.shindig.elasticsearch.util.ShindigUtil;
import org.apache.shindig.social.opensocial.spi.ActivityStreamService;
import org.apache.shindig.social.opensocial.spi.MessageService;
import org.apache.shindig.social.websockbackend.spi.IExtPersonService;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Factory constructing crawlers using injected configuration and service
 * objects to circumvent the limitations of Guice's dependency injection.
 * (a simple constructed list can not be injected directly, since its elements
 * still require injected services to function)
 * 
 * Currently included: Person Crawler, ActivityStreams Crawler, Message Crawler
 */
@Singleton
public class CrawlerFactory
{
    private static final String PROFILES_ON = "shindig.elasticsearch.profiles.enabled";
    private static final String ACTIVITIES_ON = "shindig.elasticsearch.activities.enabled";
    private static final String MESSAGES_ON = "shindig.elasticsearch.messages.enabled";
    
    private final boolean fProfsOn, fActsOn, fMsgsOn;
    
    //TODO: extract to interface(s) for dynamic injection?
    //TODO: transition to setter injection for more services?
    
    private final List<ICrawler> fCrawlers;
    
    /**
     * Creates a new crawler factory using the given configuration, connector
     * and services to construct individual crawlers.
     * None of the parameters may be null.
     * 
     * @param config configuration object for crawlers to use
     * @param people person service for crawlers to use
     * @param activities activitystreams service for crawlers to use
     * @param messages message service for crawlers to use
     * @param esConn elasticsearch connector for crawlers to use
     * @param shindig shindig utility to use
     */
    @Inject
    public CrawlerFactory(ESConfig config, IExtPersonService people,
        ActivityStreamService activities, MessageService messages,
        IESConnector esConn, ShindigUtil shindig)
    {
        //TODO: configurability to enable and disable crawlers
        
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(people == null)
        {
            throw new NullPointerException("person service was null");
        }
        if(activities == null)
        {
            throw new NullPointerException("activitystreams service was null");
        }
        if(messages == null)
        {
            throw new NullPointerException("message service was null");
        }
        if(esConn == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        
        //check which crawlers are enabled
        fProfsOn = Boolean.parseBoolean(config.getProperty(PROFILES_ON));
        fActsOn = Boolean.parseBoolean(config.getProperty(ACTIVITIES_ON));
        fMsgsOn = Boolean.parseBoolean(config.getProperty(MESSAGES_ON));
        
        //construct and add crawlers
        fCrawlers = new ArrayList<ICrawler>();
        
        if(fProfsOn)
        {
            fCrawlers.add(new PersonCrawler(config, people, esConn, shindig));
        }
        if(fActsOn)
        {
            fCrawlers.add(new ActivityStreamsCrawler(config, activities,
                people, esConn, shindig));
        }
        if(fMsgsOn)
        {
            fCrawlers.add(new MessageCrawler(config, messages, people, esConn));
        }
    }
    
    /**
     * @return list of all activated crawlers
     */
    public List<ICrawler> getCrawlers()
    {
        return fCrawlers;
    }
}
