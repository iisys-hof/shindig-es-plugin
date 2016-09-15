package org.apache.shindig.elasticsearch.crawling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Set;

import org.apache.shindig.elasticsearch.ESConfig;
import org.apache.shindig.elasticsearch.util.IESConnector;
import org.apache.shindig.elasticsearch.util.ShindigEncoder;
import org.apache.shindig.elasticsearch.util.ShindigUtil;
import org.apache.shindig.social.opensocial.model.Person;
import org.apache.shindig.social.opensocial.spi.CollectionOptions;
import org.apache.shindig.social.opensocial.spi.UserId;
import org.apache.shindig.social.opensocial.spi.UserId.Type;
import org.apache.shindig.social.websockbackend.model.ISkillSet;
import org.apache.shindig.social.websockbackend.spi.IExtPersonService;
import org.json.JSONObject;

import com.google.inject.Inject;

/**
 * Crawler performing a full match between all user profiles found in Shindig
 * and all profiles already indexed in Elasticsearch. New, deleted and updated
 * entries are determined and the index is updated accordingly.
 */
public class PersonCrawler implements ICrawler
{
    private static final String SHINDIG_INDEX = "shindig.elasticsearch.index";
    private static final String PERSON_TYPE = "shindig.elasticsearch.person_type";
    
    private final IExtPersonService fPeople;
    
    private final IESConnector fEsConn;
    
    private final ShindigUtil fShindUtil;
    
    private final String fShindigIndex, fPersonType;
    
    private final Logger fLogger;
    
    /**
     * Creates a new person crawler, using the given configuration and person
     * service, indexing entries via the given elasticsearch connector.
     * None of the parameters may be null.
     * 
     * @param config configuration object to use
     * @param people person service to use
     * @param esConn elasticsearch connector to use
     */
    @Inject
    public PersonCrawler(ESConfig config, IExtPersonService people,
        IESConnector esConn, ShindigUtil shindig)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(people == null)
        {
            throw new NullPointerException("person service was null");
        }
        if(esConn == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        
        fPeople = people;
        fEsConn = esConn;
        fShindUtil = shindig;
        
        fShindigIndex = config.getProperty(SHINDIG_INDEX);
        fPersonType = config.getProperty(PERSON_TYPE);
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
    }

    @Override
    public void crawl()
    {
        try
        {
            //TODO: use pagination

            //retrieve IDs and changed date for all people
            Map<String, Person> localPeople = getAllPeople();
            
            //match with results from Elasticsearch
            Map<String, JSONObject> remotePeople = new HashMap<String, JSONObject>();
            List<JSONObject> esPeople = fEsConn.getAll(fShindigIndex, fPersonType);
            for(JSONObject p : esPeople)
            {
                remotePeople.put(p.getString("id"), p);
            }
            
            //check who is missing where, remove handled IDs
            handleDeleted(localPeople, remotePeople);
            
            //add new, remove handled IDs
            handleNew(localPeople, remotePeople);
            
            //handle updated entries
            handleUpdated(localPeople, remotePeople);
        }
        catch(Exception e)
        {
            fLogger.log(Level.SEVERE, "error updating profile index", e);
        }
    }
    
    private Map<String, Person> getAllPeople() throws Exception
    {
        Map<String, Person> localPeople = new HashMap<String, Person>();
        
        Set<String> fields = new HashSet<String>();
        fields.add("id");
        fields.add("updated");
        CollectionOptions options = new CollectionOptions();
        List<Person> people = fPeople.getAllPeople(options,
            fields, null).get().getList();
        
        for(Person p : people)
        {
            localPeople.put(p.getId(), p);
        }
        
        return localPeople;
    }
    
    private void handleDeleted(Map<String, Person> localPeople,
        Map<String, JSONObject> remotePeople) throws Exception
    {
        Set<String> localIds = localPeople.keySet();
        Set<String> remoteIds = remotePeople.keySet();

        //remove deleted
        List<String> deleted = new ArrayList<String>(remoteIds);
        //entries that are in the index, but not in shindig
        deleted.removeAll(localIds);
        if(!deleted.isEmpty())
        {
            fLogger.log(Level.FINER, "removing " + deleted.size()
                + " deleted people from index");
            
            fEsConn.bulkDelete(fShindigIndex, fPersonType, deleted);

            //remove from remaining set
            for(String id : deleted)
            {
                remotePeople.remove(id);
            }
        }
    }
    
    private void handleNew(Map<String, Person> localPeople,
        Map<String, JSONObject> remotePeople) throws Exception
    {
        Set<String> localIds = localPeople.keySet();
        Set<String> remoteIds = remotePeople.keySet();

        Set<String> newPeople = new HashSet<String>(localIds);
        //entries that are available locally, but not remotely
        newPeople.removeAll(remoteIds);
        if(!newPeople.isEmpty())
        {
            fLogger.log(Level.FINER, "adding " + newPeople.size()
                + " new people to index");

            //retrieve all fields  and index
            List<JSONObject> objects = retrieveWithAllFields(newPeople);
            fEsConn.bulkAdd(fShindigIndex, fPersonType, objects);
            
            //remove from remaining set
            for(String id : newPeople)
            {
                localPeople.remove(id);
            }
        }
    }

    private void handleUpdated(Map<String, Person> localPeople,
        Map<String, JSONObject> remotePeople) throws Exception
    {
        //compare "updated" timestamps on remaining entries
        Set<String> updatedPeople = new HashSet<String>();
        for(Entry<String, Person> pE : localPeople.entrySet())
        {
            Person local = pE.getValue();
            JSONObject remote = remotePeople.get(local.getId());

            //TODO: make default here configurable
            if(local.getUpdated() == null
                || remote.opt("updated") == null
                || local.getUpdated().getTime() > remote.getLong("updated"))
            {

                //TODO: logging?
                fLogger.log(Level.FINEST, "queueing update for person '"
                    + local.getId() + "' in index");
                
                updatedPeople.add(local.getId());
            }
        }

        if(!updatedPeople.isEmpty())
        {
            fLogger.log(Level.FINER, "updating " + updatedPeople.size()
                + " people in index");
            
            //retrieve all fields and index updated people
            List<JSONObject> objects = retrieveWithAllFields(updatedPeople);
            
            //TODO: compare JSON for equality first?
            //TODO: equality complicated if order does not match
            //TODO: only check if both timestamps are null
            
            fEsConn.bulkUpdate(fShindigIndex, fPersonType, objects);
        }
    }
    
    private List<JSONObject> retrieveWithAllFields(Set<String> ids) throws Exception
    {
        /*
         * retrieves a set of full profiles by their ID
         */
        Set<UserId> userIds = new HashSet<UserId>();
        for(String id : ids)
        {
            userIds.add(new UserId(Type.userId, id));
        }
        
        Set<String> fields = new HashSet<String>();
        CollectionOptions options = new CollectionOptions();
        List<Person> people = fPeople.getPeople(userIds, null, options, fields,
            null).get().getList();

        //convert to ES-compatible JSON-objects
        List<JSONObject> profiles = new ArrayList<JSONObject>(people.size());
        for(Person p : people)
        {
            // retrieve list of skills
            List<ISkillSet> skills = fShindUtil.getSkills(p.getId());
            
            // convert
            profiles.add(ShindigEncoder.toJSON(p, skills));
        }
        
        return profiles;
    }
}
