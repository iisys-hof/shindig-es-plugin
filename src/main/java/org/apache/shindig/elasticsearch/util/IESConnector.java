package org.apache.shindig.elasticsearch.util;

import java.util.List;

import org.json.JSONObject;

/**
 * Generic interface for elasticsearch connectors.
 * Caution: not all implementations will behave the identically.
 */
public interface IESConnector
{
    /**
     * Closes the connection to the Elasticsearch server.
     */
    public void close();
    
    /**
     * Checks with Elasticsearch whether the index with the given name exists.
     * The given name must not be null.
     * 
     * @param index name of the index
     * @return whether the index already exists
     * @throws Exception if the request fails
     */
    public boolean indexExists(String index) throws Exception;
    
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
        throws Exception;
    
    /**
     * Creates the index with the given name. If the index already exists,
     * the call is ignored. The given name must not be null.
     * 
     * @param index name of the index to create
     * @throws Exception if index creation fails
     */
    public void createIndex(String index) throws Exception;
    
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
        throws Exception;
    
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
        throws Exception;
    
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
        throws Exception;
    
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
        throws Exception;
    
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
    public void delete(String index, String type, String id) throws Exception;
    
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
        throws Exception;
    
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
    public List<JSONObject> getAll(String index, String type) throws Exception;
    
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
    public JSONObject get(String index, String type, String id) throws Exception;
    
    /**
     * Deletes the index with the given name completely and re-creates it.
     * 
     * @param index index to clear
     * @throws Exception if clearing the index fails
     */
    public void clearIndex(String index) throws Exception;
    
    /**
     * Sets the mapping for a type in an index.
     * None of the parameters may be null.
     * 
     * @param index name of the index
     * @param type name of the type
     * @param mapping mapping to set
     * @throws Exception if setting fails
     */
    public void setMapping(String index, String type, JSONObject mapping) throws Exception;
}
