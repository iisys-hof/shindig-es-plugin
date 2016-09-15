package org.apache.shindig.elasticsearch.util;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.ESConfig;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Elasticsearch connector utility class using the binary transport client
 * providing indexing, updating and deletion of JSON object in the index and
 * index management functionality - implementation performing lazy bulking.
 * The connection can be closed manually, but will be closed automatically on
 * shutdown.
 */
@Singleton
public class ESBulkingConnector implements BulkProcessor.Listener, IESConnector
{
    private static final String HOST_PROP = "shindig.elasticsearch.host";
    private static final String PORT_PROP = "shindig.elasticsearch.port";
    private static final String CNAME_PROP = "shindig.elasticsearch.cluster.name";

    private static final String ACTIONS_PROP = "shindig.elasticsearch.bulking.actions";
    private static final String MBS_PROP = "shindig.elasticsearch.bulking.megabytes";
    private static final String SECONDS_PROP = "shindig.elasticsearch.bulking.seconds";
    
    private final String fEsHost, fEsClusterName;
    private final int fEsPort;
    
    private final Client fClient;
    private final BulkProcessor fBulkProcessor;
    
    private final Logger fLogger;
    
    /**
     * Creates a new Elasticsearch connector, connecting to the server defined
     * by the configuration object via the transport client.
     * The given configuration object must not be null.
     * 
     * @param config configuration object to use.
     */
    @Inject
    public ESBulkingConnector(ESConfig config)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        
        fEsHost = config.getProperty(HOST_PROP);
        fEsClusterName = config.getProperty(CNAME_PROP);
        fEsPort = Integer.parseInt(config.getProperty(PORT_PROP));
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
        
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", fEsClusterName).build();
        
        fClient = new TransportClient(settings).addTransportAddress(
            new InetSocketTransportAddress(fEsHost, fEsPort));
        
        //read bulking configuration
        int actions = Integer.parseInt(config.getProperty(ACTIONS_PROP));
        long megabytes = Long.parseLong(config.getProperty(MBS_PROP));
        long seconds = Long.parseLong(config.getProperty(SECONDS_PROP));
        
        //create bulk processor
        fBulkProcessor = BulkProcessor.builder(fClient, this)
            //execute every ... requests
            .setBulkActions(actions)
            //execute every ... Megabytes
            .setBulkSize(new ByteSizeValue(megabytes, ByteSizeUnit.MB))
            //flush every ... seconds
            .setFlushInterval(TimeValue.timeValueSeconds(seconds))
            //n concurrent requests while bulking
            .setConcurrentRequests(1)
            .build();
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request)
    {
        fLogger.log(Level.FINEST, "beforeBulk: " + executionId
            + " - " + request);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
        BulkResponse response)
    {
        fLogger.log(Level.FINEST, "afterBulk success: " + executionId
            + " - " + request + " - " + response);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
        Throwable failure)
    {
        fLogger.log(Level.SEVERE, "afterBulk fail: " + executionId
            + " - " + request, failure);
    }
    
    /**
     * Closes the connection to the Elasticsearch server.
     */
    public void close()
    {
        fBulkProcessor.close();
        fClient.close();
    }
    
    /**
     * Checks with Elasticsearch whether the index with the given name exists.
     * The given name must not be null.
     * 
     * @param index name of the index
     * @return whether the index already exists
     * @throws Exception if the request fails
     */
    public boolean indexExists(String index) throws Exception
    {
        boolean exists = false;
        
        try
        {
            IndicesExistsResponse er = fClient.admin().indices().exists(
                new IndicesExistsRequest(index)).actionGet();
            exists = er.isExists();
        }
        catch(IndexMissingException e)
        {
            fLogger.log(Level.SEVERE, "'index exists' request is borked", e);
        }
        
        return exists;
    }
    
    /**
     * Checks whether a document entry in the given index, with the given type
     * and ID already exists.
     * None of the parameters may be null.
     * 
     * @param index name of the index to query
     * @param type type of the document entry to look for
     * @param id ID of the document entry to look for
     * @return whether the entry already exists
     * @throws Exception if the request fails
     */
    public boolean entryExists(String index, String type, String id)
        throws Exception
    {
        boolean exists = false;
        
        try
        {
            JSONObject entry = this.get(index, type, id);
            if(entry != null)
            {
                exists = true;
            }
        }
        catch(DocumentMissingException e)
        {
            //document not found, false is default
        }
        
        return exists;
    }
    
    /**
     * Creates the index with the given name. If the index already exists,
     * the call is ignored. The given name must not be null.
     * 
     * @param index name of the index to create
     * @throws Exception if index creation fails
     */
    public void createIndex(String index) throws Exception
    {
        try
        {
            fClient.admin().indices().create(
                Requests.createIndexRequest(index)).actionGet();
            
            //TODO: evaluate response?
        }
        catch(IndexAlreadyExistsException e)
        {
            fLogger.log(Level.WARNING, "index '"
                + index + "' already exists", e);
        }
    }
    
    /**
     * Adds an entry in JSON form to the specified index under the specified
     * type and ID. A check whether the index exists is executed and if the
     * index does not exist, it will be created.
     * Existing entries with matching index, type and ID will be overwritten.
     * None of the parameters may be null.
     * 
     * @param index name of the index to add the entry to
     * @param type type of the entry to add
     * @param id ID of the entry to add
     * @param entry source of the entry to add to the index
     * @throws Exception if adding the entry fails
     */
    public void add(String index, String type, String id, JSONObject entry)
        throws Exception
    {
        //make sure index exists to avoid Exception
        //TODO: don't check before every call?
        boolean exists = indexExists(index);
        
        if(!exists)
        {
            createIndex(index);
        }
        
        fBulkProcessor.add(
            fClient.prepareIndex(index, type, id)
            .setSource(entry.toString()).request());
    }
    
    /**
     * Adds a list of entries of the given type to the specified index.
     * A check whether the index exists is executed and if the index does not
     * exist, it will be created.
     * Existing entries with matching index, type and ID will be overwritten.
     * None of the parameters may be null.
     * 
     * @param index name of the index to add entries to
     * @param type type of the entries to add
     * @param entries entries to add
     * @throws Exception if adding entries fails
     */
    public void bulkAdd(String index, String type, List<JSONObject> entries)
        throws Exception
    {
        //make sure index exists to avoid Exception
        //TODO: don't check before every call?
        boolean exists = indexExists(index);
        
        if(!exists)
        {
            createIndex(index);
        }
        
        //queue index requests
        for(JSONObject entry : entries)
        {
            fBulkProcessor.add(
                fClient.prepareIndex(index, type, entry.getString("id"))
                .setSource(entry.toString()).request());
        }
        
    }
    
    /**
     * Updates an entry with the given type and ID in JSON form in the
     * specified index. A check whether the index exists is executed and if the
     * index does not exist, it will be created.
     * This will fail if the entry does not already exist.
     * None of the parameters may be null.
     * 
     * @param index name of the index to update the entry in
     * @param type type of the entry to update
     * @param id ID of the entry to update
     * @param entry updated source of the entry to update in the index
     * @throws Exception if updating the entry fails
     */
    public void update(String index, String type, String id, JSONObject entry)
        throws Exception
    {
        //make sure index exists to avoid Exception
        //TODO: don't check before every call?
        boolean exists = indexExists(index);
        
        if(!exists)
        {
            //TODO: remove? request will fail anyway
            createIndex(index);
        }
        
        fBulkProcessor.add(fClient.prepareUpdate(index, type, id)
            .setDoc(entry.toString()).request());
    }
    
    /**
     * Updates a list of entries of the given type in the specified index.
     * A check whether the index exists is executed and if the index does not
     * exist, it will be created.
     * Existing entries with matching index, type and ID will be overwritten.
     * None of the parameters may be null.
     * 
     * @param index name of the index to update entries in
     * @param type type of the entries to update
     * @param entries entries to update
     * @throws Exception if updating entries fails
     */
    public void bulkUpdate(String index, String type, List<JSONObject> entries)
        throws Exception
    {
        //make sure index exists to avoid Exception
        //TODO: don't check before every call?
        boolean exists = indexExists(index);
        
        if(!exists)
        {
            createIndex(index);
        }
        
        //queue update requests
        for(JSONObject entry : entries)
        {
            fBulkProcessor.add(fClient.prepareUpdate(index, type,
                entry.getString("id")).setDoc(entry.toString()).request());
        }
    }
    
    /**
     * Deletes an entry with the given type and ID from the specified index.
     * The call is ignored, if the index does not exist.
     * The call will fail if the entry does not exist.
     * None of the parameters may be null.
     * 
     * @param index index to remove the entry from
     * @param type type of the entry to remove
     * @param id ID of the entry to remove
     * @throws Exception if removing the entry fails
     */
    public void delete(String index, String type, String id) throws Exception
    {
        //make sure index exists to avoid Exception
        //TODO: don't check before every call?
        boolean exists = indexExists(index);
        
        if(exists)
        {

            fBulkProcessor.add(
                fClient.prepareDelete(index, type, id).request());

            //TODO: evaluate answer?
        }
        else
        {
            //TODO: log?
        }
    }
    
    /**
     * Deletes a list of entries with the given type from the specified index,
     * defined by a list of entry IDs.
     * The call is ignored, if the index does not exist.
     * None of the parameters may be null.
     * 
     * @param index index to remove entries from
     * @param type type of the entries to remove
     * @param ids IDs of entries to remove
     * @throws Exception if removing the entries fails
     */
    public void bulkDelete(String index, String type, List<String> ids)
        throws Exception
    {
        //make sure index exists to avoid Exception
        //TODO: don't check before every call?
        boolean exists = indexExists(index);
        
        if(exists)
        {
            //queue deletion requests
            for(String id : ids)
            {
                fBulkProcessor.add(
                    fClient.prepareDelete(index, type, id).request());
            }
        }
    }
    
    /**
     * Retrieves all entries of the specified type from the specified index.
     * If the index does not exist, an emtpy list is returned.
     * None of the parameters may be null.
     * 
     * @param index name of the index to query
     * @param type type of the entries to retrieve
     * @return list of all available entries
     * @throws Exception if the request fails
     */
    public List<JSONObject> getAll(String index, String type) throws Exception
    {
        List<JSONObject> results = new ArrayList<JSONObject>();
        boolean exists = indexExists(index);
        
        if(exists)
        {
            SearchResponse response = fClient.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(Integer.MAX_VALUE).execute()
                .actionGet();

            //TODO: pagination?
//              .setFrom(0).setSize(60)
            
            SearchHit[] hits = response.getHits().getHits();
            
            for(SearchHit hit : hits)
            {
                results.add(new JSONObject(hit.getSourceAsString()));
            }
        }
        
        return results;
    }
    
    /**
     * Retrieves a single entry with the given ID and type from the specified
     * index.
     * If the index does not exist, null is returned.
     * None of the parameters may be null.
     * 
     * @param index name of the index to query
     * @param type type of the entry to retrieve
     * @param id ID of the entry to retrieve
     * @return JSON representation of the entry
     * @throws Exception if the request fails
     */
    public JSONObject get(String index, String type, String id) throws Exception
    {
        JSONObject result = null;
        
        //make sure index exists to avoid Exception
        boolean exists = indexExists(index);
        
        if(exists)
        {
            GetResponse response = fClient.prepareGet(index, type, id)
                .execute().actionGet();
            
            //TODO: what happens if the entry does not exist?
            
            if(response.getSourceAsString() != null)
            {
                result = new JSONObject(response.getSourceAsString());
            }
        }

        return result;
    }

    @Override
    public void clearIndex(String index) throws Exception
    {
        //drop index
        if(indexExists(index))
        {
            fClient.admin().indices().delete(
                Requests.deleteIndexRequest(index)).actionGet();
        }
        
        //create index
        createIndex(index);
    }

    @Override
    public void setMapping(String index, String type, JSONObject mapping)
        throws Exception
    {
        //check if index exists, create if not
        if(!indexExists(index))
        {
          fClient.admin().indices().prepareCreate(index)
              .addMapping(type, mapping.toString())
              .execute().actionGet();
        }
        else
        {
            //update mapping
            fClient.admin().indices().preparePutMapping(index)
                .setIgnoreConflicts(true)
                .setType(type).setSource(mapping.toString())
                .execute().actionGet();
        }
    }
}