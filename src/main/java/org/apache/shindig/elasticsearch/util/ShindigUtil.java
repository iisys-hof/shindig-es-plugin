package org.apache.shindig.elasticsearch.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.shindig.social.opensocial.model.Person;
import org.apache.shindig.social.opensocial.spi.CollectionOptions;
import org.apache.shindig.social.opensocial.spi.GroupId;
import org.apache.shindig.social.opensocial.spi.UserId;
import org.apache.shindig.social.opensocial.spi.UserId.Type;
import org.apache.shindig.social.websockbackend.model.ISkillSet;
import org.apache.shindig.social.websockbackend.spi.IExtPersonService;
import org.apache.shindig.social.websockbackend.spi.ISkillService;

import com.google.inject.Singleton;

/**
 * Utility class providing functions querying Apache Shindig used by indexing
 * mechanisms.
 */
@Singleton
public class ShindigUtil
{
    private final IExtPersonService fPeople;
    private final ISkillService fSkills;
    
    private final Set<String> fIdField;
    private final CollectionOptions fCollOpts;
    private final GroupId fFriendsGroup;
    
    /**
     * Creates a new shindig utility querying the given services.
     * None of the parameters may be null.
     * 
     * @param people person service to use
     */
    @Inject
    public ShindigUtil(IExtPersonService people, ISkillService skills)
    {
        if(people == null)
        {
            throw new NullPointerException("person service was null");
        }
        if(skills == null)
        {
            throw new NullPointerException("skill service was null");
        }
        
        fPeople = people;
        fSkills = skills;
        
        //query presets
        fIdField = new HashSet<String>();
        fIdField.add("id");
        fCollOpts = new CollectionOptions();
        fFriendsGroup = new GroupId(GroupId.Type.friends, "@friends");
    }

    public Person getPerson(String userId) throws Exception
    {
        return fPeople.getPerson(new UserId(UserId.Type.userId, userId),
            null, null).get();
    }
    
    /**
     * Retrieves a list of all friends of the user with the specified ID.
     * The given ID must not be null.
     * 
     * @param userId ID of the user to retrieve all friends of
     * @return list of user IDs who are friends with the user
     * @throws Exception if the query fails
     */
    public List<String> getAllFriends(String userId) throws Exception
    {
        Set<UserId> userIds = new HashSet<UserId>();
        userIds.add(new UserId(Type.userId, userId));
        
        List<Person> people = fPeople.getPeople(userIds, fFriendsGroup,
            fCollOpts, fIdField, null).get().getList();

        List<String> friends = new ArrayList<String>(people.size());
        for(Person p : people)
        {
            friends.add(p.getId());
        }

        return friends;
    }
    
    /**
     * Retrieves a list of skills including their links for a userId.
     * 
     * @param userId ID of the user
     * @return a complete set of skills
     */
    public List<ISkillSet> getSkills(String userId) throws Exception
    {
        // retrieve list of skills
        UserId uid = new UserId(UserId.Type.userId, userId);
        
        // TODO: better error handling
        List<ISkillSet> skills = fSkills.getSkills(uid, fCollOpts, null)
            .get().getList();
        
        return skills;
    }
}
