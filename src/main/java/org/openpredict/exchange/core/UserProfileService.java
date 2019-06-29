package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.StateHash;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.beans.cmd.CommandResultCode;

import java.util.Objects;

/**
 * Stateful (!) User profile service
 * <p>
 * TODO make multi instance
 */
@Slf4j
public final class UserProfileService implements WriteBytesMarshallable, StateHash {

    /**
     * State: uid -> user profile
     */
    private final LongObjectHashMap<UserProfile> userProfiles;

    public UserProfileService() {
        this.userProfiles = new LongObjectHashMap<>();
    }

    public UserProfileService(BytesIn bytes) {
        this.userProfiles = Utils.readLongHashMap(bytes, UserProfile::new);
    }

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
     * Perform balance adjustment for specific user
     *
     * @param uid
     * @param currency
     * @param amount
     * @param fundingTransactionId
     * @return result code
     */
    public CommandResultCode balanceAdjustment(final long uid, final int currency, final long amount, final long fundingTransactionId) {

        final UserProfile userProfile = getUserProfile(uid);
        if (userProfile == null) {
            log.warn("User profile {} not found", uid);
            return CommandResultCode.AUTH_INVALID_USER;
        }

        if (amount == 0) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ZERO;
        }

        // double settlement protection
        if (userProfile.externalTransactions.contains(fundingTransactionId)) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED;
        }

        // validate balance for withdrowals
        if (amount < 0 && (userProfile.accounts.get(currency) + amount < 0)) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_NSF;
        }

        userProfile.externalTransactions.add(fundingTransactionId);
        userProfile.accounts.addToValue(currency, amount);

        //log.debug("FUND: {}", userProfile);
        return CommandResultCode.SUCCESS;
    }

    /**
     * Create a new user profile with known unique uid
     *
     * @param uid
     * @return
     */
    public CommandResultCode addEmptyUserProfile(long uid) {
        if (userProfiles.get(uid) == null) {
            userProfiles.put(uid, new UserProfile(uid));
            return CommandResultCode.SUCCESS;
        } else {
            log.debug("Can not add user, already exists: {}", uid);
            return CommandResultCode.USER_MGMT_USER_ALREADY_EXISTS;
        }
    }

    /**
     * Serialize user profile
     *
     * @param uid   user id
     * @param bytes bytes to write into
     * @return true if user found, false otherwise
     */
    public boolean singleUserState(final long uid, final BytesOut bytes) {
        final UserProfile userProfile = userProfiles.get(uid);
        if (userProfile != null) {
            userProfile.writeMarshallable(bytes);
            return true;
        } else {
            return false;
        }
    }

    public void reset() {
        userProfiles.clear();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        // write symbolSpecs
        Utils.marshallLongHashMap(userProfiles, bytes);
    }

    @Override
    public int stateHash() {
        return Objects.hash(Utils.stateHash(userProfiles));
    }

}