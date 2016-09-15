package org.apache.shindig.elasticsearch.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.shindig.elasticsearch.ESConfig;
import org.apache.shindig.elasticsearch.util.IESConnector;
import org.apache.shindig.elasticsearch.util.ShindigEncoder;
import org.apache.shindig.elasticsearch.util.ShindigUtil;
import org.apache.shindig.social.opensocial.model.ActivityEntry;
import org.apache.shindig.social.opensocial.model.Message;
import org.apache.shindig.social.opensocial.model.Person;
import org.apache.shindig.social.websockbackend.events.IEventListener;
import org.apache.shindig.social.websockbackend.events.IShindigEvent;
import org.apache.shindig.social.websockbackend.events.ShindigEventBus;
import org.apache.shindig.social.websockbackend.events.ShindigEventType;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Listener for changes regarding profiles, activities and messages making,
 * relaying any changes to an elasticsearch server.
 */
@Singleton
public class ElasticsearchListener implements IEventListener
{
    private static final String HANDLE_EVENTS = "shindig.elasticsearch.handle_events";
    private static final String SHINDIG_INDEX = "shindig.elasticsearch.index";

    private static final String PERSON_TYPE = "shindig.elasticsearch.person_type";
    private static final String ACTIVITY_TYPE = "shindig.elasticsearch.activity_type";
    private static final String MESSAGE_TYPE = "shindig.elasticsearch.message_type";
    
    private static final String PROFILES_ON = "shindig.elasticsearch.profiles.enabled";
    private static final String ACTIVITIES_ON = "shindig.elasticsearch.activities.enabled";
    private static final String MESSAGES_ON = "shindig.elasticsearch.messages.enabled";
    
    private static final String SKILLS_ON = "shindig.elasticsearch.skills.enabled";
    
    private static final String ADD_FRIEND_ACL = "shindig.elasticsearch.acls.add_friends";
    
    private final IESConnector fConn;
    
    private final ShindigUtil fShindUtil;
    
    private final String fIndex;
    
    private final String fPersonType, fActivityType, fMessageType;
    
    private final boolean fProfsOn, fActsOn, fMsgsOn, fSkillsOn, fAddFriendAcl;
    private final boolean fEnabled;
    
    private final Logger fLogger;
    
    /**
     * Creates a shindig event listener with the given configuration relaying
     * changes to elasticsearch via the given connector.
     * Registers itself to the given event bus
     * None of the parameters may be null.
     * 
     * @param config configuration object to use
     * @param connector elasticsearch connector to use
     * @param eventBus event bus to register to
     * @param shindig shindig utility to use
     */
    @Inject
    public ElasticsearchListener(ESConfig config, IESConnector connector,
        ShindigEventBus eventBus, ShindigUtil shindig)
    {
        if(config == null)
        {
            throw new NullPointerException("configuration object was null");
        }
        if(connector == null)
        {
            throw new NullPointerException("elasticsearch connector was null");
        }
        if(eventBus == null)
        {
            throw new NullPointerException("event bus was null");
        }
        if(shindig == null)
        {
            throw new NullPointerException("shindig utility was null");
        }
        
        fEnabled = Boolean.parseBoolean(config.getProperty(HANDLE_EVENTS));
        fIndex = config.getProperty(SHINDIG_INDEX);
        
        fPersonType = config.getProperty(PERSON_TYPE);
        fActivityType = config.getProperty(ACTIVITY_TYPE);
        fMessageType = config.getProperty(MESSAGE_TYPE);
        
        fConn = connector;
        fShindUtil = shindig;
        
        fProfsOn = Boolean.parseBoolean(config.getProperty(PROFILES_ON));
        fActsOn = Boolean.parseBoolean(config.getProperty(ACTIVITIES_ON));
        fMsgsOn = Boolean.parseBoolean(config.getProperty(MESSAGES_ON));
        
        fSkillsOn = Boolean.parseBoolean(config.getProperty(SKILLS_ON));

        fAddFriendAcl = Boolean.parseBoolean(config.getProperty(ADD_FRIEND_ACL));
        
        fLogger = Logger.getLogger(this.getClass().getCanonicalName());
        

        //register to event bus
        eventBus.addListener(ShindigEventType.ALL, this);
    }
    
    
    @Override
    public void handleEvent(IShindigEvent event)
    {
        if(!fEnabled)
        {
            return;
        }
        
        try
        {
            switch(event.getType())
            {
                case ACTIVITY_CREATED:
                    activityCreated(event);
                    break;
                case ACTIVITY_UPDATED:
                    activityUpdated(event);
                    break;
                case ACTIVITY_DELETED:
                    activityDeleted(event);
                    break;
                    
                case PROFILE_CREATED:
                    profileCreated(event);
                    break;
                case PROFILE_UPDATED:
                    profileUpdated(event);
                    break;
                case PROFILE_DELETED:
                    profileDeleted(event);
                    break;

                case MESSAGE_CREATED:
                    messageCreated(event);
                    break;
                case MESSAGE_UPDATED:
                    messageUpdated(event);
                    break;
                case MESSAGE_DELETED:
                    messageDeleted(event);
                    break;
                    
                case SKILL_ADDED:
                    skillsChanged(event);
                    break;
                case SKILL_REMOVED:
                    skillsChanged(event);
                    break;
            }
        }
        catch(Exception e)
        {
            fLogger.log(Level.SEVERE, "could not update index", e);
        }
    }
    
    private ActivityEntry toActivity(Object payload)
    {
        return (ActivityEntry) payload;
    }
    
    private Person toPerson(Object payload)
    {
        //TODO: include extra fields?
        return (Person) payload;
    }
    
    private Message toMessage(Object payload)
    {
        return (Message) payload;
    }
    
    private JSONObject toJSON(ActivityEntry entry, Map<String, String> props)
        throws Exception
    {
        JSONObject json = ShindigEncoder.toJSON(entry);
        
        //add metadata
        if(props != null)
        {
            json.put("origin", props.get("userId"));
            json.put("groupId", props.get("groupId"));
            json.put("appId", props.get("appId"));
        }
        
        return json;
    }
    
    private JSONObject toJSON(Message message, Map<String, String> props)
        throws Exception
    {
        JSONObject json = ShindigEncoder.toJSON(message);
        
        //add metadata
        if(props != null)
        {
            json.put("origin", props.get("userId"));
            json.put("messageCollection", props.get("messageCollectionId"));
            json.put("appId", props.get("appId"));
        }
        
        return json;
    }
    
    private JSONArray getFriendsACL(Map<String, String> props)
    {
        JSONArray friendArr = null;
        
        if(props != null && props.get("userId") != null)
        {
            try
            {
                String userId = props.get("userId");
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
        }
        
        return friendArr;
    }

    private void activityCreated(IShindigEvent event) throws Exception
    {
        if(fActsOn)
        {
            ActivityEntry entry = toActivity(event.getPayload());
            
            //convert to json
            JSONObject activity = toJSON(entry, event.getProperties());
            
            if(fAddFriendAcl)
            {
                //add friends list as ACL
                JSONArray acl = getFriendsACL(event.getProperties());
                activity.put("whitelist", acl);
            }
            
            //index in Elasticsearch
            fConn.add(fIndex, fActivityType, entry.getId(), activity);
        }
    }

    private void activityUpdated(IShindigEvent event) throws Exception
    {
        if(fActsOn)
        {
            ActivityEntry entry = toActivity(event.getPayload());
            
            //update index
            JSONObject activity = toJSON(entry, event.getProperties());
            
            //index in Elasticsearch
            String id = entry.getId();
            
            if(fConn.entryExists(fIndex, fActivityType, id))
            {
                //update it if it exists
                fConn.update(fIndex, fActivityType, id, activity);
            }
            else
            {
                //add it if it does not exist
                fConn.add(fIndex, fActivityType, id, activity);
            }
        }
    }

    private void activityDeleted(IShindigEvent event) throws Exception
    {
        if(fActsOn)
        {
            ActivityEntry entry = toActivity(event.getPayload());
            
            //remove from Elasticsearch
            if(fConn.entryExists(fIndex, fActivityType, entry.getId()))
            {
                fConn.delete(fIndex, fActivityType, entry.getId());
            }
        }
    }
    
    private void profileCreated(IShindigEvent event) throws Exception
    {
        if(fProfsOn)
        {
            Person person = toPerson(event.getPayload());
            
            //add to index
            JSONObject profile = ShindigEncoder.toJSON(person,
                fShindUtil.getSkills(person.getId()));
            
            //index in Elasticsearch
            fConn.add(fIndex, fPersonType, person.getId(), profile);
        }
    }
    
    private void profileUpdated(IShindigEvent event) throws Exception
    {
        if(fProfsOn)
        {
            Person person = toPerson(event.getPayload());
            
            //update index
            JSONObject profile = ShindigEncoder.toJSON(person,
                fShindUtil.getSkills(person.getId()));
            
            //index in Elasticsearch
            String id = person.getId();
            
            if(fConn.entryExists(fIndex, fPersonType, id))
            {
                //update it if it exists
                fConn.update(fIndex, fPersonType, id, profile);
            }
            else
            {
                //add it if it does not exist
                fConn.add(fIndex, fPersonType, id, profile);
            }
        }
    }
    
    private void profileDeleted(IShindigEvent event) throws Exception
    {
        if(fProfsOn)
        {
            Person person = toPerson(event.getPayload());
            
            //remove from Elasticsearch
            if(fConn.entryExists(fIndex, fPersonType, person.getId()))
            {
                fConn.delete(fIndex, fPersonType, person.getId());
            }
        }
    }
    
    private JSONArray getMessageOrigin(Message message) throws Exception
    {
        JSONArray origin = null;

        //add recipients if the message has already been sent
        List<String> recipients = message.getRecipients();
        if(message.getTimeSent() != null
            && recipients != null && !recipients.isEmpty())
        {
            //recipients as base array if available
            origin = ShindigEncoder.toArray(recipients);
        }
        else
        {
            origin = new JSONArray();
        }
        
        //sender
        origin.put(message.getSenderId());
        
        return origin;
    }
    
    private void messageCreated(IShindigEvent event) throws Exception
    {
        if(fMsgsOn)
        {
            Message message = toMessage(event.getPayload());
            
            //add to index
            JSONObject entry = toJSON(message, event.getProperties());
            
            //TODO: construct origin from sender and recipients?
            JSONArray origin = getMessageOrigin(message);
            entry.put("origin", origin);
            
            //TODO: add marker whether message was already sent
            
            fConn.add(fIndex, fMessageType, message.getId(), entry);
        }
    }
    
    private void messageUpdated(IShindigEvent event) throws Exception
    {
        if(fMsgsOn)
        {
            Message message = toMessage(event.getPayload());
            
            //update in index
            JSONObject entry = toJSON(message, event.getProperties());
            
            
            String id = message.getId();
            if(fConn.entryExists(fIndex, fMessageType, id))
            {
                //TODO: preserve origin
                JSONObject oldEntry = fConn.get(fIndex, fMessageType, id);
                JSONArray oldOrigin = (JSONArray) oldEntry.remove("origin");
                
                //TODO: check for updated senders and recipients? can they be updated?
                entry.put("origin", oldOrigin);
                
                fConn.update(fIndex, fMessageType, id, entry);
            }
            else
            {
                //TODO: construct origin from sender and recipients?
                JSONArray origin = getMessageOrigin(message);
                entry.put("origin", origin);
                
                //TODO: causes a problem for messages that have been deleted by
                //others already
                
                fConn.add(fIndex, fMessageType, id, entry);
            }
        }
    }
    
    private void messageDeleted(IShindigEvent event) throws Exception
    {
        if(fMsgsOn)
        {
            Message message = toMessage(event.getPayload());
            
            //remove from Elasticsearch
            if(fConn.entryExists(fIndex, fMessageType, message.getId()))
            {
                //TODO: only update, removing the user from the "origin" list
                String userId = event.getProperties().get("userId");
                
                //TODO: test ... how?
                
                //get existing entry
                JSONObject oldEntry = fConn.get(fIndex, fMessageType, message.getId());
                
                JSONArray oldOrigin = (JSONArray) oldEntry.remove("origin");
                //entries can not be removed, rebuild
                List<String> originUsers = new ArrayList<String>(oldOrigin.length());
                for(int i = 0; i < oldOrigin.length(); ++i)
                {
                    String id = oldOrigin.getString(i);
                    if(!id.equals(userId))
                    {
                        originUsers.add(id);
                    }
                }
                oldEntry.put("origin", ShindigEncoder.toArray(originUsers));
                
                if(!originUsers.isEmpty())
                {
                    fConn.update(fIndex, fMessageType, message.getId(), oldEntry);
                }
                else
                {
                    //TODO: actually delete if there are no owners left?
                    fConn.delete(fIndex, fMessageType, message.getId());
                }
            }
        }
    }
    
    private void skillsChanged(IShindigEvent event) throws Exception
    {
        if(fSkillsOn)
        {
            String userId = ((String[]) event.getPayload())[0];
            
            Person person = fShindUtil.getPerson(userId);
            
            //update index
            JSONObject profile = ShindigEncoder.toJSON(person,
                fShindUtil.getSkills(userId));
            
            //index in Elasticsearch
            String id = person.getId();
            
            if(fConn.entryExists(fIndex, fPersonType, id))
            {
                //update it if it exists
                fConn.update(fIndex, fPersonType, id, profile);
            }
            else
            {
                //add it if it does not exist
                fConn.add(fIndex, fPersonType, id, profile);
            }
        }
    }
}
