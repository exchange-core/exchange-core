package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.UserProfile;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UserProfileService {

    private MutableLongObjectMap<UserProfile> userProfiles = new LongObjectHashMap<>();

    public UserProfile getUserProfile(long uid) {
        return userProfiles.get(uid);
    }

    public boolean addEmptyUserProfile(long uid) {
        if (userProfiles.get(uid) != null) {
            log.debug("Can not add user, already exists: {}", uid);
            return false;
        }
        userProfiles.put(uid, new UserProfile(uid));
        return true;
    }

    // TESTING only
    public void reset() {
        for (Object v : userProfiles.values()) {
            if (v != null) {
                ((UserProfile) v).clear();
            }
        }
    }

}