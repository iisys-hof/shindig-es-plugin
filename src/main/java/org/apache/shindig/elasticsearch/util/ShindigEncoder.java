package org.apache.shindig.elasticsearch.util;

import java.util.List;

import org.apache.shindig.social.opensocial.model.ActivityEntry;
import org.apache.shindig.social.opensocial.model.ActivityObject;
import org.apache.shindig.social.opensocial.model.ListField;
import org.apache.shindig.social.opensocial.model.Message;
import org.apache.shindig.social.opensocial.model.Name;
import org.apache.shindig.social.opensocial.model.Organization;
import org.apache.shindig.social.opensocial.model.Person;
import org.apache.shindig.social.opensocial.model.Url;
import org.apache.shindig.social.websockbackend.model.IExtOrganization;
import org.apache.shindig.social.websockbackend.model.ISkillSet;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Utility class used for encoding Shindig's domain object into JSON that can
 * be indexed in Elasticsearch.
 */
public class ShindigEncoder
{
    /**
     * Converts the given activitystreams entry to JSON.
     * The given entry must not be null.
     * 
     * @param entry activitystreams entry to convert
     * @return JSON representation of the entry
     * @throws Exception if conversion fails
     */
    public static JSONObject toJSON(ActivityEntry entry) throws Exception
    {
        //TODO: add owner ID of the original ActivityStream?
        
        JSONObject activity = new JSONObject();
        
        activity.put("id", entry.getId());
        activity.put("_name", entry.getTitle());
        activity.put("title", entry.getTitle());
        activity.put("content", entry.getContent());
        activity.put("published", entry.getPublished());
        activity.put("updated", entry.getUpdated());
        activity.put("url", entry.getUrl());
        activity.put("verb", entry.getVerb());
        
        // additional fields for search compatibility
        // nuxeo version
        activity.put("dc:title", entry.getTitle());
        activity.put("dc:created", entry.getPublished());
        // liferay version
        activity.put("title", entry.getTitle());
        activity.put("createDate", entry.getPublished());
        
        //subordinate objects
        addActObj(entry.getActor(), "actor", activity);
        addActObj(entry.getObject(), "object", activity);
        addActObj(entry.getTarget(), "target", activity);
        addActObj(entry.getGenerator(), "generator", activity);
        addActObj(entry.getProvider(), "provider", activity);
        
        return activity;
    }
    
    private static void addActObj(ActivityObject actObj, String key, JSONObject activity)
        throws Exception
    {
        if(actObj != null)
        {
            JSONObject object = new JSONObject();
            
            object.put("id", actObj.getId());
            object.put("_name", actObj.getDisplayName());
            object.put("displayName", actObj.getDisplayName());
            object.put("content", actObj.getContent());
            object.put("objectType", actObj.getObjectType());
            object.put("published", actObj.getPublished());
            object.put("summary", actObj.getSummary());
            object.put("updated", actObj.getUpdated());
            object.put("url", actObj.getUrl());
            
            // additional fields for search compatibility
            // nuxeo version
            object.put("dc:title", actObj.getDisplayName());
            object.put("dc:created", actObj.getPublished());
            // liferay version
            object.put("title", actObj.getDisplayName());
            object.put("createDate", actObj.getPublished());
            
            activity.put(key, object);
        }
    }
    
    /**
     * Converts the given person profile to JSON.
     * The given person must not be null.
     * 
     * @param person person profile to convert
     * @return JSON representation of the profile
     * @throws Exception if conversion fails
     */
    public static JSONObject toJSON(Person person, List<ISkillSet> skills) throws Exception
    {
        //TODO: visibility?
        //TODO: addresses, org addresses?
        
        JSONObject profile = new JSONObject();
        
        profile.put("id", person.getId());
        
        //TODO: naming alternatives?
        profile.put("_name", person.getDisplayName());
        profile.put("displayName", person.getDisplayName());
        
        profile.put("aboutMe", person.getAboutMe());
        profile.put("age", person.getAge());
        if(person.getBirthday() != null)
        {
            profile.put("birthday", person.getBirthday().getTime());
        }
        profile.put("jobInterests", person.getJobInterests());
        profile.put("nickname", person.getNickname());
        profile.put("preferredUsername", person.getPreferredUsername());
        profile.put("profileUrl", person.getProfileUrl());
        profile.put("relationshipStatus", person.getRelationshipStatus());
        
        //TODO: exclude because it's temporary?
        profile.put("status", person.getStatus());
        
        profile.put("thumbnailUrl", person.getThumbnailUrl());
        if(person.getUpdated() != null)
        {
            profile.put("updated", person.getUpdated().getTime());
        }
        
        // additional fields for search compatibility
        // nuxeo version
        profile.put("dc:title", person.getDisplayName());
        // liferay version
        profile.put("title", person.getDisplayName());
        
        //subordinate objects
        profile.put("emails", lflToArray(person.getEmails()));
        profile.put("IMs", lflToArray(person.getIms()));
        profile.put("interests", toArray(person.getInterests()));
        profile.put("languagesSpoken", toArray(person.getLanguagesSpoken()));
        
        JSONObject name = new JSONObject();
        Name n = person.getName();
        if(n != null)
        {
            name.put("additionalName", n.getAdditionalName());
            name.put("familyName", n.getFamilyName());
            name.put("formatted", n.getFormatted());
            name.put("givenName", n.getGivenName());
            name.put("honorificPrefix", n.getHonorificPrefix());
            name.put("honorificSuffix", n.getHonorificSuffix());
        }
        profile.put("name", name);

        //TODO: exclude because it's temporary?
        if(person.getNetworkPresence() != null)
        {
            profile.put("networkPresence",
                person.getNetworkPresence().getDisplayValue());
        }
        
        if(person.getOrganizations() != null)
        {
            JSONArray organizations = new JSONArray();
            for(Organization o : person.getOrganizations())
            {
                JSONObject org = new JSONObject();
                
                //TODO: address?
                org.put("description", o.getDescription());
                if(o.getStartDate() != null)
                {
                    org.put("startDate", o.getStartDate().getTime());
                }
                if(o.getEndDate() != null)
                {
                    org.put("endDate", o.getEndDate().getTime());
                }
                org.put("field", o.getField());
                org.put("name", o.getName());
                org.put("primary", o.getPrimary());
                org.put("salary", o.getSalary());
                org.put("subField", o.getSubField());
                org.put("title", o.getTitle());
                org.put("type", o.getType());
                org.put("webpage", o.getWebpage());
                
                // extended model
                if(o instanceof IExtOrganization)
                {
                    IExtOrganization extO = (IExtOrganization) o;
                    
                    org.put("managerId", extO.getManagerId());
                    org.put("secretaryId", extO.getSecretaryId());
                    org.put("department", extO.getDepartment());
                    org.put("departmentHead", extO.isDepartmentHead());
                    org.put("orgUnit", extO.getOrgUnit());
                    org.put("location", extO.getLocation());
                    org.put("site", extO.getSite());
                }
                
                organizations.put(org);
            }
            profile.put("organizations", organizations);
        }
        
        profile.put("phoneNumbers", lflToArray(person.getPhoneNumbers()));
        profile.put("tags", toArray(person.getTags()));
        
        if(person.getUrls() != null)
        {
            JSONArray urls = new JSONArray();
            
            for(Url url : person.getUrls())
            {
                JSONObject entry = new JSONObject();
                
                entry.put("linkText", url.getLinkText());
                entry.put("primary", url.getPrimary());
                entry.put("type", url.getType());
                entry.put("value", url.getValue());
                
                urls.put(entry);
            }
            
            profile.put("urls", urls);
        }
        
        // skills from skill service
        if(skills != null && !skills.isEmpty())
        {
            JSONArray skillArr = new JSONArray();
            
            for(ISkillSet set : skills)
            {
                skillArr.put(set.getName());
            }
            
            profile.put("skills", skillArr);
        }
        
        return profile;
    }
    
    /**
     * Converts a list of Strings to a JSONArray of Strings.
     * If the given list is null, null will be returned.
     * 
     * @param list list to convert to JSON
     * @return JSON representation of the list
     * @throws Exception if conversion fails
     */
    public static JSONArray toArray(List<String> list) throws Exception
    {
        if(list == null)
        {
            return null;
        }
        
        JSONArray array = new JSONArray();
        
        for(String s : list)
        {
            array.put(s);
        }
        
        return array;
    }
    
    /**
     * Converts one of Shindig's list field lists to JSON.
     * If the given list is null, null will be returned.
     * 
     * @param lfl list of list fields to convert
     * @return JSON representation of the list field list
     * @throws Exception if conversion fails
     */
    public static JSONArray lflToArray(List<ListField> lfl) throws Exception
    {
        if(lfl == null)
        {
            return null;
        }
        
        JSONArray array = new JSONArray();
        
        for(ListField field : lfl)
        {
            JSONObject entry = new JSONObject();
            entry.put("value", field.getValue());
            entry.put("type", field.getType());
            entry.put("primary", field.getPrimary());
            array.put(entry);
        }
        
        return array;
    }
    
    /**
     * Converts the given message to JSON.
     * The given message must not be null.
     * 
     * @param message message to convert
     * @return JSON representation of the message
     * @throws Exception if conversion fails
     */
    public static JSONObject toJSON(Message message) throws Exception
    {
        JSONObject entry = new JSONObject();
        
        entry.put("appUrl", message.getAppUrl());
        entry.put("body", message.getBody());
        entry.put("bodyId", message.getBodyId());
        entry.put("collectionIds", toArray(message.getCollectionIds()));
        entry.put("id", message.getId());
        entry.put("inReplyTo", message.getInReplyTo());
        entry.put("recipients", toArray(message.getRecipients()));
        entry.put("replies", toArray(message.getReplies()));
        entry.put("senderId", message.getSenderId());
        
        if(message.getStatus() != null)
        {
            entry.put("status", message.getStatus().name());
        }

        if(message.getTimeSent() != null)
        {
            entry.put("timeSent", message.getTimeSent().getTime());
        }

        entry.put("title", message.getTitle());
        entry.put("titleId", message.getTitleId());
        
        if(message.getType() != null)
        {
            entry.put("type", message.getType().name());
        }
        
        if(message.getUpdated() != null)
        {
            entry.put("updated", message.getUpdated().getTime());
        }
        
        if(message.getUrls() != null)
        {
            JSONArray urls = new JSONArray();
            
            for(Url url : message.getUrls())
            {
                JSONObject e = new JSONObject();
                
                e.put("linkText", url.getLinkText());
                e.put("primary", url.getPrimary());
                e.put("type", url.getType());
                e.put("value", url.getValue());
                
                urls.put(e);
            }
            
            entry.put("urls", urls);
        }
        
        // additional fields for search compatibility
        // nuxeo version
        entry.put("dc:title", message.getTitle());
        if(message.getTimeSent() != null)
        {
            entry.put("dc:created", message.getTimeSent().getTime());
        }
        // liferay version
        entry.put("title", message.getTitle());
        if(message.getTimeSent() != null)
        {
            entry.put("createDate", message.getTimeSent().getTime());
        }
        
        return entry;
    }
}
