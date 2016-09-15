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

import org.apache.shindig.common.util.DateUtil;
import org.apache.shindig.elasticsearch.ESConfig;
import org.apache.shindig.elasticsearch.util.IESConnector;
import org.apache.shindig.elasticsearch.util.ShindigEncoder;
import org.apache.shindig.elasticsearch.util.ShindigUtil;
import org.apache.shindig.social.opensocial.model.ActivityEntry;
import org.apache.shindig.social.opensocial.model.Person;
import org.apache.shindig.social.opensocial.spi.ActivityStreamService;
import org.apache.shindig.social.opensocial.spi.CollectionOptions;
import org.apache.shindig.social.opensocial.spi.UserId;
import org.apache.shindig.social.opensocial.spi.UserId.Type;
import org.apache.shindig.social.websockbackend.spi.IExtPersonService;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.inject.Inject;

/**
 * Crawler performing a full match between all activities found in Shindig and
 * all activities already indexed in Elasticsearch. New, deleted and updated
 * entries are determined and the index is updated accordingly.
 */
public class ActivityStreamsCrawler implements ICrawler
{
    private static final String SHINDIG_INDEX = "shindig.elasticsearch.index";
    private static final String ACTIVITY_TYPE = "shindig.elasticsearch.activity_type";
    
    private static final String ADD_FRIEND_ACL = "shindig.elasticsearch.acls.add_friends";
    
    private final ActivityStreamService fActivities;
    private final IExtPersonService fPeople;
    
    private final IESConnector fEsConn;
    
    private final ShindigUtil fShindUtil;
    
    private final String fShindigIndex, fActivityType;
    
    private final boolean fAddFriendAcl;
    
    private final Logger fLogger;
    
    /**
     * Creates a new activity streams crawler, using the given configuration,
     * activitystreams and person services, indexing entries via the given
     * elasticsearch connector.
     * None of the parameters may be null.
     * 
     * @param config configuration object to use
     * @param activities activity streams service to use
     * @param people person service to use
     * @param esConn elasticsearch connector to use
     */
    @Inject
    public ActivityStreamsCrawler(ESConfig config, ActivityStreamService activities,
        IExtPersonService people, IESConnector esConn, ShindigUtil shindig)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(activities == null)
        {
            throw new NullPointerException("activity stream service was null");
        }
        if(people == null)
        {
            throw new NullPointerException("person service was null");
        }
        if(esConn == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        if(shindig == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        
        fActivities = activities;
        fPeople = people;
        fEsConn = esConn;
        
        fShindUtil = shindig;
        
        fShindigIndex = config.getProperty(SHINDIG_INDEX);
        fActivityType = config.getProperty(ACTIVITY_TYPE);
        
        fAddFriendAcl = Boolean.parseBoolean(
            config.getProperty(ADD_FRIEND_ACL));
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
    }

    @Override
    public void crawl()
    {
        try
        {
            //TODO: use pagination
            Map<String, String> activityOwners = new HashMap<String, String>();
            Map<String, Set<String>> actsByOwner = new HashMap<String, Set<String>>();
            
            //retrieve all activities for all people from Shindig
            Map<String, ActivityEntry> localActs = readAllActivities(activityOwners, actsByOwner);
            
            //match with results from Elasticsearch
            Map<String, JSONObject> remoteActs = new HashMap<String, JSONObject>();
            List<JSONObject> esActs = fEsConn.getAll(fShindigIndex, fActivityType);
            for(JSONObject activity : esActs)
            {
                remoteActs.put(activity.getString("id"), activity);
            }
            
            //track deleted and new, remove them from lists
            handleDeleted(localActs, activityOwners, actsByOwner, remoteActs);
            handleNew(localActs, activityOwners, actsByOwner, remoteActs);
            
            //handle updated entries
            handleUpdates(localActs, activityOwners, actsByOwner, remoteActs);
        }
        catch(Exception e)
        {
            fLogger.log(Level.SEVERE, "error updating activity index", e);
        }
    }
    
    private Map<String, ActivityEntry> readAllActivities(
        Map<String, String> activityOwners,
        Map<String, Set<String>> actsByOwner) throws Exception
    {
        Map<String, ActivityEntry> localActs = new HashMap<String, ActivityEntry>();
        
        CollectionOptions options = new CollectionOptions();
        
        //query for all people
        Set<String> idField = new HashSet<String>();
        idField.add("id");
        List<Person> people = fPeople.getAllPeople(options,
            idField, null).get().getList();

        //prepare activitystreams query to shindig
        Set<String> fields = new HashSet<String>();
        fields.add("id");
        //TODO: can activities even be updated?
        fields.add("updated");
        fields.add("published");
        
        //retrieve all people's own activity streams
        for(Person p : people)
        {
            Set<UserId> userIds = new HashSet<UserId>();
            userIds.add(new UserId(Type.userId, p.getId()));
            
            //get all activities for this person
            List<ActivityEntry> activities =
                fActivities.getActivityEntries(userIds, null, null, fields,
                options, null).get().getList();
            
            //sort into local collections
            for(ActivityEntry entry : activities)
            {
                localActs.put(entry.getId(), entry);
                addToOwner(p.getId(), entry.getId(), activityOwners, actsByOwner);
            }
        }
        
        return localActs;
    }
    
    private void handleDeleted(Map<String, ActivityEntry> localActs,
        Map<String, String> activityOwners,
        Map<String, Set<String>> actsByOwner,
        Map<String, JSONObject> remoteActs) throws Exception
    {
        Set<String> localIds = localActs.keySet();
        Set<String> remoteIds = remoteActs.keySet();

        //remove deleted
        List<String> deleted = new ArrayList<String>(remoteIds);
        //entries that are in the index, but not in shindig
        deleted.removeAll(localIds);
        if(!deleted.isEmpty())
        {
            fLogger.log(Level.FINER, "removing " + deleted.size()
                + " deleted activities from index");

            fEsConn.bulkDelete(fShindigIndex, fActivityType, deleted);

            //remove from remaining set
            for(String id : deleted)
            {
                remoteActs.remove(id);
                removeFromOwner(activityOwners.get(id), id,
                    activityOwners, actsByOwner);
            }
        }
    }
    
    private JSONArray getFriendsACL(String userId)
    {
        JSONArray friendArr = null;
        
        try
        {
            List<String> friends = fShindUtil.getAllFriends(userId);
            friendArr = ShindigEncoder.toArray(friends);
            
            //put original user on whitelist
            friendArr.put(userId);
        }
        catch(Exception e)
        {
            fLogger.log(Level.WARNING,
                "could not retrieve friend ACL", e);
        }
        
        return friendArr;
    }
    
    private void handleNew(Map<String, ActivityEntry> localActs,
        Map<String, String> activityOwners,
        Map<String, Set<String>> actsByOwner,
        Map<String, JSONObject> remoteActs) throws Exception
    {
        Set<String> localIds = localActs.keySet();
        Set<String> remoteIds = remoteActs.keySet();
        
        //add new
        Set<String> newActs = new HashSet<String>(localIds);
        //entries that are available locally, but not remotely
        newActs.removeAll(remoteIds);
        if(!newActs.isEmpty())
        {
            fLogger.log(Level.FINER, "adding " + newActs.size()
                + " new activities to index");
            
            //sort by owner for retrieval
            Map<String, Set<String>> newByOwner
                = new HashMap<String, Set<String>>();
            for(String id : newActs)
            {
                Set<String> acts = newByOwner.get(activityOwners.get(id));
                if(acts == null)
                {
                    acts = new HashSet<String>();
                    newByOwner.put(activityOwners.get(id), acts);
                }
                acts.add(id);
            }
            
            //collect all new activities
            List<JSONObject> newObjs = new ArrayList<JSONObject>();
            for(Entry<String, Set<String>> aE : newByOwner.entrySet())
            {
                List<JSONObject> objects = retrieveWithAllFields(
                    aE.getKey(), aE.getValue());
                
                //add ACLs (friends) if configured
                if(fAddFriendAcl)
                {
                    JSONArray friends = getFriendsACL(aE.getKey());
                    
                    for(JSONObject activity : objects)
                    {
                        activity.put("whitelist", friends);
                    }
                }
                
                newObjs.addAll(objects);
            }
            
            //bulk add
            fEsConn.bulkAdd(fShindigIndex, fActivityType, newObjs);
            
            //remove from remaining collections
            for(String id : newActs)
            {
                localActs.remove(id);
                removeFromOwner(activityOwners.get(id), id,
                    activityOwners, actsByOwner);
            }
        }
    }
    
    private void handleUpdates(Map<String, ActivityEntry> localActs,
        Map<String, String> activityOwners,
        Map<String, Set<String>> actsByOwner,
        Map<String, JSONObject> remoteActs) throws Exception
    {
        //compare "updated" timestamps on remaining entries
        List<JSONObject> updatedActs = new LinkedList<JSONObject>();
        for(Entry<String, Set<String>> aE : actsByOwner.entrySet())
        {
            String userId = aE.getKey();
            
            for(String id : aE.getValue())
            {
                ActivityEntry local = localActs.get(id);
                JSONObject remote = remoteActs.get(id);
                
                //remove all that weren't updated
                if(!wasUpdated(local, remote))
                {
                    removeFromOwner(userId, id, activityOwners,
                        actsByOwner);
                }
            }
            
            //retrieve and convert remaining entries for this user
            updatedActs.addAll(retrieveWithAllFields(userId, aE.getValue()));
        }
        
        //execute bulk update for all users' activities
        if(!updatedActs.isEmpty())
        {
            fLogger.log(Level.FINER, "updating " + updatedActs.size()
                + " activities in index");
            
            //TODO: compare JSON for equality first?
            //TODO: equality complicated if order does not match
            //TODO: only check if both timestamps are null
            
            fEsConn.bulkUpdate(fShindigIndex, fActivityType, updatedActs);
        }
    }
    
    private void addToOwner(String ownerId, String entryId,
        Map<String, String> activityOwners, Map<String, Set<String>> actsByOwner)
    {
        //add to collections enabling lookups
        //owners by entry IDs
        activityOwners.put(entryId, ownerId);
        
        //entries by owner ID
        Set<String> entries = actsByOwner.get(ownerId);
        if(entries == null)
        {
            entries = new HashSet<String>();
            
        }
        entries.add(entryId);
    }
    
    private void removeFromOwner(String ownerId, String entryId,
        Map<String, String> activityOwners, Map<String, Set<String>> actsByOwner)
    {
        /*
         * remove from collections enabling lookups, preventing unnecessary or
         * wrong operations
         */
        activityOwners.remove(entryId);
        
        Set<String> list = actsByOwner.get(ownerId);
        if(list != null)
        {
            list.remove(entryId);
        }
    }
    
    private List<JSONObject> retrieveWithAllFields(String userId,
        Set<String> ids) throws Exception
    {
        /*
         * retrieves a set of activities by their ID for the given user
         */
        Set<String> fields = new HashSet<String>();
        CollectionOptions options = new CollectionOptions();
        
        List<ActivityEntry> entries = fActivities.getActivityEntries(
            new UserId(Type.userId, userId), null, null, fields, options, ids,
            null).get().getList();

        //convert to ES-compatible JSON-objects
        List<JSONObject> acts = new ArrayList<JSONObject>(entries.size());
        for(ActivityEntry entry : entries)
        {
            JSONObject json = ShindigEncoder.toJSON(entry);
            json.put("origin", userId);
            acts.add(json);
        }
        
        return acts;
    }
    
    private JSONObject retrieveWithAllFields(String userId, String id) throws Exception
    {
        ActivityEntry activity = fActivities.getActivityEntry(
            new UserId(Type.userId, userId),
            null, null, null, id, null).get();
        
        return ShindigEncoder.toJSON(activity);
    }
    
    private boolean wasUpdated(ActivityEntry local, JSONObject remote)
    {
        //compares activities' timestamps, which are ISO 8601 encoded
        //TODO: make default here configurable
        //TODO: can activities even be updated? only compare "published"?
        boolean updated = false;
        
        //convert timestamps to usable format
        String localU = local.getUpdated();
        String remoteU = remote.optString("updated");
        
        //return "not updated" of there are no timestamps
        if(localU != null && remoteU != null)
        {
            Long localTime = getTime(localU);
            Long remoteTime = getTime(remoteU);
            
            if(localTime > remoteTime)
            {
                updated = true;
            }
        }
        
        return updated;
    }
    
    private Long getTime(String timestamp)
    {
        //returns unix timestamps for ISO 8601 timestamps
        Long time = null;
        
        try
        {
            if(timestamp != null && !timestamp.isEmpty())
            {
                time = DateUtil.parseIso8601DateTime(timestamp).getTime();
            }
        }
        catch(Exception e)
        {
            //TODO: logging?
//            fLogger.log(Level.SEVERE, "timestamp translation went wrong", e);
        }
        
        return time;
    }
}
