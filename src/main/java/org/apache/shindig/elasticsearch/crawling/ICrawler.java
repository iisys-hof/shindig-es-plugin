package org.apache.shindig.elasticsearch.crawling;

/**
 * Interface for a generic self-organized crawler that can be triggered in
 * regular intervals.
 */
public interface ICrawler
{
    /**
     * Performs a full crawl operation.
     */
    public void crawl();
}
