package org.apache.shindig.elasticsearch.crawling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.ESConfig;
import org.apache.shindig.elasticsearch.util.IESConnector;
import org.apache.shindig.elasticsearch.util.ShindigEncoder;
import org.apache.shindig.social.opensocial.model.Message;
import org.apache.shindig.social.opensocial.model.Person;
import org.apache.shindig.social.opensocial.spi.CollectionOptions;
import org.apache.shindig.social.opensocial.spi.MessageService;
import org.apache.shindig.social.opensocial.spi.UserId;
import org.apache.shindig.social.opensocial.spi.UserId.Type;
import org.apache.shindig.social.websockbackend.spi.IExtPersonService;
import org.json.JSONObject;

import com.google.inject.Inject;

/**
 * Crawler performing a full match between all user profiles found in Shindig
 * and all profiles already indexed in Elasticsearch. New, deleted and updated
 * entries are determined and the index is updated accordingly.
 * Not threadsafe
 */
public class MessageCrawler implements ICrawler
{
    private static final String SHINDIG_INDEX = "shindig.elasticsearch.index";
    private static final String MESSAGE_TYPE = "shindig.elasticsearch.message_type";
    
    private final MessageService fMessages;
    private final IExtPersonService fPeople;
    
    private final IESConnector fEsConn;
    
    private final String fShindigIndex, fMessageType;
    
    private final Logger fLogger;
    
    private final Map<String, String> fPrimaryMsgOwners;
    private final Map<String, List<String>> fAllMsgOwners;
    private final Map<String, List<String>> fMsgsByOwner;
    
    /**
     * Creates a new message crawler, using the given configuration, message
     * and person services, indexing entries via the given elasticsearch
     * connector.
     * None of the parameters may be null.
     * 
     * @param config configuration object to use
     * @param messages message service to use
     * @param people person service to use
     * @param esConn elasticsearch connector to use
     */
    @Inject
    public MessageCrawler(ESConfig config, MessageService messages,
        IExtPersonService people, IESConnector esConn)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(messages == null)
        {
            throw new NullPointerException("message service was null");
        }
        if(people == null)
        {
            throw new NullPointerException("person service was null");
        }
        if(esConn == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        
        fMessages = messages;
        fPeople = people;
        fEsConn = esConn;
        
        fShindigIndex = config.getProperty(SHINDIG_INDEX);
        fMessageType = config.getProperty(MESSAGE_TYPE);
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());

        //temporary lookips
        fPrimaryMsgOwners = new HashMap<String, String>();
        fAllMsgOwners = new HashMap<String, List<String>>();
        fMsgsByOwner = new HashMap<String, List<String>>();
    }

    @Override
    public void crawl()
    {
        try
        {
            //TODO: use pagination
            //TODO: global deduplication or one entry per person?

            //retrieve all messages
            Map<String, Message> localMessages = getAllMessages();
            
            //match with results from Elasticsearch
            Map<String, JSONObject> remoteMessages = new HashMap<String, JSONObject>();
            List<JSONObject> esMsgs = fEsConn.getAll(fShindigIndex, fMessageType);
            for(JSONObject m : esMsgs)
            {
                remoteMessages.put(m.getString("id"), m);
            }

            //track deleted and new, remove handled IDs
            handleDeleted(localMessages, remoteMessages);
            handleNew(localMessages, remoteMessages);
            
            //handle updated entries
            handleUpdated(localMessages, remoteMessages);
        }
        catch(Exception e)
        {
            fLogger.log(Level.SEVERE, "error updating message index", e);
        }
        
        //cleanup
        fPrimaryMsgOwners.clear();
        fAllMsgOwners.clear();
        fMsgsByOwner.clear();
    }
    
    private Map<String, Message> getAllMessages() throws Exception
    {
        Map<String, Message> localMessages = new HashMap<String, Message>();
        CollectionOptions options = new CollectionOptions();

        //query for all people
        Set<String> idField = new HashSet<String>();
        idField.add("id");
        List<Person> people = fPeople.getAllPeople(options,
            idField, null).get().getList();

        //prepare message query to shindig
        Set<String> fields = new HashSet<String>();
        fields.add("id");
        fields.add("timeSent");
        fields.add("updated");

        //retrieve all people's message collections' messages
        for(Person p : people)
        {
            //TODO: message collection IDs?
            List<Message> msgs = fMessages.getMessages(
                new UserId(Type.userId, p.getId()), null,
                fields, null, options, null).get().getList();
            
            for(Message m : msgs)
            {
                localMessages.put(m.getId(), m);
                addToOwner(p.getId(), m.getId());
            }
        }
        
        return localMessages;
    }
    
    private void handleDeleted(Map<String, Message> localMessages,
        Map<String, JSONObject> remoteMessages) throws Exception
    {
        Set<String> localIds = localMessages.keySet();
        Set<String> remoteIds = remoteMessages.keySet();
        
        //remove deleted
        List<String> deleted = new ArrayList<String>(remoteIds);
        //entries that are in the index, but not in shindig
        deleted.removeAll(localIds);
        if(!deleted.isEmpty())
        {
            fLogger.log(Level.FINER, "removing " + deleted.size()
                + " deleted messages from index");
            
            fEsConn.bulkDelete(fShindigIndex, fMessageType, deleted);
            
            //remove from remaining set
            for(String id : deleted)
            {
                remoteMessages.remove(id);
                removeFromOwner(fPrimaryMsgOwners.get(id), id);
            }
        }
    }
    
    private void handleNew(Map<String, Message> localMessages,
        Map<String, JSONObject> remoteMessages) throws Exception
    {
        Set<String> localIds = localMessages.keySet();
        Set<String> remoteIds = remoteMessages.keySet();
        
        //check for and add new entries
        Set<String> newMessages = new HashSet<String>(localIds);
        //entries that are available locally, but not remotely
        newMessages.removeAll(remoteIds);
        if(!newMessages.isEmpty())
        {
            fLogger.log(Level.FINER, "adding " + newMessages.size()
                + " new messages to index");
            
            //sort by owner for retrieval
            Map<String, List<String>> newByOwner
                = new HashMap<String, List<String>>();
            for(String id : newMessages)
            {
                List<String> msgs = newByOwner.get(fPrimaryMsgOwners.get(id));
                if(msgs == null)
                {
                    msgs = new ArrayList<String>();
                    newByOwner.put(fPrimaryMsgOwners.get(id), msgs);
                }
                msgs.add(id);
            }
            
            //collect all new activities
            List<JSONObject> newObjs = new ArrayList<JSONObject>();
            for(Entry<String, List<String>> mE : newByOwner.entrySet())
            {
                List<JSONObject> objects = retrieveWithAllFields(
                    mE.getKey(), mE.getValue());
                newObjs.addAll(objects);
            }

            //bulk add
            fEsConn.bulkAdd(fShindigIndex, fMessageType, newObjs);
            
            //remove from remaining collections
            for(String id : newMessages)
            {
                localMessages.remove(id);
                removeFromOwner(fPrimaryMsgOwners.get(id), id);
            }
        }
    }
    
    private void handleUpdated(Map<String, Message> localMessages,
        Map<String, JSONObject> remoteMessages) throws Exception
    {
        //compare "updated" timestamps on remaining entries
        List<JSONObject> updatedMsgs = new LinkedList<JSONObject>();
        for(Entry<String, List<String>> mE : fMsgsByOwner.entrySet())
        {
            String userId = mE.getKey();
            
            for(String id : mE.getValue())
            {
                Message local = localMessages.get(id);
                JSONObject remote = remoteMessages.get(id);
                
                //remove all that weren't updated
                //TODO: can messages even be updated? only compare "sentDate"?
                if(!(local.getUpdated() == null
                    || remote.opt("updated") == null
                    || local.getUpdated().getTime() > remote.getLong("updated")))
                {
                    removeFromOwner(userId, id);
                }
            }
            
            //retrieve and convert entries for this user
            updatedMsgs.addAll(retrieveWithAllFields(userId, mE.getValue()));
        }

        //execute bulk update
        if(!updatedMsgs.isEmpty())
        {
            fLogger.log(Level.FINER, "updating " + updatedMsgs.size()
                + " messages in index");
            
            //TODO: compare JSON for equality first?
            //TODO: equality complicated if order does not match
            //TODO: only check if both timestamps are null
            
            fEsConn.bulkUpdate(fShindigIndex, fMessageType, updatedMsgs);
        }
    }
    
    private void addToOwner(String ownerId, String entryId)
    {
        //add to collections enabling lookups
        //primary owners by entry IDs
        fPrimaryMsgOwners.put(entryId, ownerId);
        
        //collect all further users
        List<String> users = fAllMsgOwners.get(entryId);
        if(users == null)
        {
            users = new ArrayList<String>();
            fAllMsgOwners.put(entryId, users);
        }
        users.add(ownerId);
        
        //entries by owner ID
        List<String> entries = fMsgsByOwner.get(ownerId);
        if(entries == null)
        {
            entries = new ArrayList<String>();
            
        }
        entries.add(entryId);
    }
    
    private void removeFromOwner(String ownerId, String entryId)
    {
        /*
         * remove from collections enabling lookups, preventing unnecessary or
         * wrong operations
         */
        fPrimaryMsgOwners.remove(entryId);
        
        List<String> list = fMsgsByOwner.get(ownerId);
        if(list != null)
        {
            list.remove(entryId);
        }
    }

    private List<JSONObject> retrieveWithAllFields(String userId,
        List<String> msgIds) throws Exception
    {
        /*
         * retrieves a set of message their ID for the given user
         */
        List<Message> msgs = fMessages.getMessages(new UserId(Type.userId, userId),
            null, null, msgIds, new CollectionOptions(), null).get().getList();


        //convert to ES-compatible JSON-objects
        List<JSONObject> messObjs = new ArrayList<JSONObject>(msgs.size());
        for(Message m : msgs)
        {
            JSONObject json = ShindigEncoder.toJSON(m);
            
            //assign to all owning users
            List<String> users = fAllMsgOwners.get(m.getId());
            json.put("origin", ShindigEncoder.toArray(users));
            
            //TODO: somehow add message collection ID?
            messObjs.add(json);
        }
        
        return messObjs;
    }

    private JSONObject retrieveWithAllFields(String userId, String id) throws Exception
    {
        List<String> msgIds = new ArrayList<String>();
        msgIds.add(id);
        
        //TODO: message collection ID?
        List<Message> msgs = fMessages.getMessages(new UserId(Type.userId, userId),
            null, null, msgIds, new CollectionOptions(), null).get().getList();
        Message message = msgs.get(0);
        
        return ShindigEncoder.toJSON(message);
    }
}
