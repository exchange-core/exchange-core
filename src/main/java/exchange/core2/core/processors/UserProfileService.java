/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.processors;

import exchange.core2.core.common.StateHash;
import exchange.core2.core.common.UserProfile;
import exchange.core2.core.common.UserStatus;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.utils.HashingUtils;
import exchange.core2.core.utils.SerializationUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

/**
 * Stateful (!) User profile service
 * <p>
 * TODO make multi instance
 */
@Slf4j
public final class UserProfileService implements WriteBytesMarshallable, StateHash {

    /*
     * State: uid to UserProfile
     */
    @Getter
    private final LongObjectHashMap<UserProfile> userProfiles;

    public UserProfileService() {
        this.userProfiles = new LongObjectHashMap<>(1024);
    }

    public UserProfileService(BytesIn bytes) {
        this.userProfiles = SerializationUtils.readLongHashMap(bytes, UserProfile::new);
    }

    /**
     * Find user profile
     *
     * @param uid uid
     * @return user profile
     */
    public UserProfile getUserProfile(long uid) {
        return userProfiles.get(uid);
    }

    public UserProfile getUserProfileOrAddSuspended(long uid) {
        return userProfiles.getIfAbsentPut(uid, () -> new UserProfile(uid, UserStatus.SUSPENDED));
    }


    /**
     * Perform balance adjustment for specific user
     *
     * @param uid                  uid
     * @param currency             account currency
     * @param amount               balance difference
     * @param fundingTransactionId transaction id (should increment only)
     * @return result code
     */
    public CommandResultCode balanceAdjustment(final long uid, final int currency, final long amount, final long fundingTransactionId) {

        final UserProfile userProfile = getUserProfile(uid);
        if (userProfile == null) {
            log.warn("User profile {} not found", uid);
            return CommandResultCode.AUTH_INVALID_USER;
        }

//        if (amount == 0) {
//            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ZERO;
//        }

        // double settlement protection
        if (userProfile.adjustmentsCounter == fundingTransactionId) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_SAME;
        }
        if (userProfile.adjustmentsCounter > fundingTransactionId) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_ALREADY_APPLIED_MANY;
        }

        // validate balance for withdrawals
        if (amount < 0 && (userProfile.accounts.get(currency) + amount < 0)) {
            return CommandResultCode.USER_MGMT_ACCOUNT_BALANCE_ADJUSTMENT_NSF;
        }

        userProfile.adjustmentsCounter = fundingTransactionId;
        userProfile.accounts.addToValue(currency, amount);

        //log.debug("FUND: {}", userProfile);
        return CommandResultCode.SUCCESS;
    }

    /**
     * Create a new user profile with known unique uid
     *
     * @param uid uid
     * @return true if user was added
     */
    public boolean addEmptyUserProfile(long uid) {
        if (userProfiles.get(uid) == null) {
            userProfiles.put(uid, new UserProfile(uid, UserStatus.ACTIVE));
            return true;
        } else {
            log.debug("Can not add user, already exists: {}", uid);
            return false;
        }
    }

    /**
     * Suspend removes inactive clients profile from the core in order to increase performance.
     * Account balances should be first adjusted to zero with BalanceAdjustmentType=SUSPEND.
     * No open margin positions allowed in the suspended profile.
     * However in some cases profile can come back with positions and non-zero balances,
     * if pending orders or pending commands was not processed yet.
     * Therefore resume operation must be able to merge profile.
     *
     * @param uid client id
     * @return result code
     */
    public CommandResultCode suspendUserProfile(long uid) {
        final UserProfile userProfile = userProfiles.get(uid);
        if (userProfile == null) {
            return CommandResultCode.USER_MGMT_USER_NOT_FOUND;

        } else if (userProfile.userStatus == UserStatus.SUSPENDED) {
            return CommandResultCode.USER_MGMT_USER_ALREADY_SUSPENDED;

        } else if (userProfile.positions.anySatisfy(pos -> !pos.isEmpty())) {
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDABLE_HAS_POSITIONS;

        } else if (userProfile.accounts.anySatisfy(acc -> acc != 0)) {
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDABLE_NON_EMPTY_ACCOUNTS;

        } else {
            log.debug("Suspended user profile: {}", userProfile);
            userProfiles.remove(uid);
            // TODO pool UserProfile objects
            return CommandResultCode.SUCCESS;
        }
    }

    public CommandResultCode resumeUserProfile(long uid) {
        final UserProfile userProfile = userProfiles.get(uid);
        if (userProfile == null) {
            // create new empty user profile
            // account balance adjustments should be applied later
            userProfiles.put(uid, new UserProfile(uid, UserStatus.ACTIVE));
            return CommandResultCode.SUCCESS;
        } else if (userProfile.userStatus != UserStatus.SUSPENDED) {
            // attempt to resume non-suspended account (or resume twice)
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDED;
        } else {
            // resume existing suspended profile (can contain non empty positions or accounts)
            userProfile.userStatus = UserStatus.ACTIVE;
            log.debug("Resumed user profile: {}", userProfile);
            return CommandResultCode.SUCCESS;
        }
    }

    /**
     * Reset module - for testing only
     */
    public void reset() {
        userProfiles.clear();
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        // write symbolSpecs
        SerializationUtils.marshallLongHashMap(userProfiles, bytes);
    }

    @Override
    public int stateHash() {
        return HashingUtils.stateHash(userProfiles);
    }

}