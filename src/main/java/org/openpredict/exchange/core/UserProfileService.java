package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.UserProfile;

/**
 * Stateful (!) User profile service
 * <p>
 * TODO make multi instance
 */
@Slf4j
public final class UserProfileService {

    /**
     * State: uid -> user profile
     */
    private MutableLongObjectMap<UserProfile> userProfiles = new LongObjectHashMap<>();

    /**
     * Find user profile
     *
     * @param uid
     * @return
     */
    public UserProfile getUserProfile(long uid) {
        return userProfiles.get(uid);
    }

    public UserProfile getUserProfileOrThrowEx(long uid) {

        final UserProfile userProfile = userProfiles.get(uid);

        if (userProfile == null) {
            throw new IllegalStateException("User profile not found, uid=" + uid);
        }

        return userProfile;
    }


    /**
     * Create a new user profile with known unique uid
     *
     * @param uid
     * @return
     */
    public boolean addEmptyUserProfile(long uid) {
        if (userProfiles.get(uid) != null) {
            log.debug("Can not add user, already exists: {}", uid);
            return false;
        }
        userProfiles.put(uid, new UserProfile(uid));
        return true;
    }

    /**
     * Reset - TESTING only
     */
    public void reset() {
        userProfiles.clear();
//        for (Object v : userProfiles.values()) {
//            if (v != null) {
//                ((UserProfile) v).clear();
//            }
//        }
    }

}