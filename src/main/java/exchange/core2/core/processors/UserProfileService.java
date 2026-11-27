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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import exchange.core2.collections.hashtable.UuidObjectHashtable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Stateful (!) User profile service
 * <p>
 * TODO make multi instance
 */
public final class UserProfileService implements WriteBytesMarshallable, StateHash {

    private static final Logger log = LoggerFactory.getLogger(UserProfileService.class);

    /*
     * State: user UUID to UserProfile
     */
    private final UuidObjectHashtable<UserProfile> userProfiles;

    public UserProfileService() {
        this.userProfiles = new UuidObjectHashtable<>(1024);
    }

    public UserProfileService(BytesIn bytes) {
        final int size = bytes.readInt();
        this.userProfiles = new UuidObjectHashtable<>(size);
        for (int i = 0; i < size; i++) {
            final UserProfile profile = new UserProfile(bytes);
            userProfiles.put(profile.uidHi, profile.uidLo, profile);
        }
    }

    /**
     * Find user profile
     *
     * @param uidHi user UUID, most significant bits
     * @param uidLo user UUID, least significant bits
     * @return user profile
     */
    public UserProfile getUserProfile(long uidHi, long uidLo) {
        return userProfiles.get(uidHi, uidLo);
    }

    public UserProfile getUserProfileOrAddSuspended(long uidHi, long uidLo) {
        return userProfiles.computeIfAbsent(uidHi, uidLo, (hi, lo) -> new UserProfile(hi, lo, UserStatus.SUSPENDED));
    }


    /**
     * Perform balance adjustment for specific user
     *
     * @param uidHi user UUID, most significant bits
     * @param uidLo user UUID, least significant bits
     * @param currency             account currency
     * @param amount               balance difference
     * @param fundingTransactionId transaction id (should increment only)
     * @return result code
     */
    public CommandResultCode balanceAdjustment(final long uidHi, final long uidLo, final int currency, final long amount, final long fundingTransactionId) {

        final UserProfile userProfile = getUserProfile(uidHi, uidLo);
        if (userProfile == null) {
            log.warn("User profile {} not found", new UUID(uidHi, uidLo));
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
        userProfile.accounts.addAndGet(currency, amount);

        //log.debug("FUND: {}", userProfile);
        return CommandResultCode.SUCCESS;
    }

    /**
     * Create a new user profile with known unique uid
     *
     * @param uidHi user UUID, most significant bits
     * @param uidLo user UUID, least significant bits
     * @return true if user was added
     */
    public boolean addEmptyUserProfile(long uidHi, long uidLo) {
        if (userProfiles.get(uidHi, uidLo) == null) {
            userProfiles.put(uidHi, uidLo, new UserProfile(uidHi, uidLo, UserStatus.ACTIVE));
            return true;
        } else {
            log.debug("Can not add user, already exists: {}", new UUID(uidHi, uidLo));
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
     * @param uidHi user UUID, most significant bits
     * @param uidLo user UUID, least significant bits
     * @return result code
     */
    public CommandResultCode suspendUserProfile(long uidHi, long uidLo) {
        final UserProfile userProfile = userProfiles.get(uidHi, uidLo);
        if (userProfile == null) {
            return CommandResultCode.USER_MGMT_USER_NOT_FOUND;

        } else if (userProfile.userStatus == UserStatus.SUSPENDED) {
            return CommandResultCode.USER_MGMT_USER_ALREADY_SUSPENDED;

        } else if (userProfile.anyPosition(pos -> !pos.isEmpty())) {
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDABLE_HAS_POSITIONS;

        } else if (userProfile.accounts.valuesStream().anyMatch(acc -> acc != 0)) {
            return CommandResultCode.USER_MGMT_USER_NOT_SUSPENDABLE_NON_EMPTY_ACCOUNTS;

        } else {
            log.debug("Suspended user profile: {}", userProfile);
            userProfiles.remove(uidHi, uidLo);
            // TODO pool UserProfile objects
            return CommandResultCode.SUCCESS;
        }
    }

    public CommandResultCode resumeUserProfile(long uidHi, long uidLo) {
        final UserProfile userProfile = userProfiles.get(uidHi, uidLo);
        if (userProfile == null) {
            // create new empty user profile
            // account balance adjustments should be applied later
            userProfiles.put(uidHi, uidLo, new UserProfile(uidHi, uidLo, UserStatus.ACTIVE));
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
     * @param uidHi user UUID, most significant bits
     * @param uidLo user UUID, least significant bits
     * @return result code
     */
    public CommandResultCode removeUserProfile(long uidHi, long uidLo) {
        final UserProfile userProfile = userProfiles.get(uidHi, uidLo);
        if (userProfile == null) {
            return CommandResultCode.USER_MGMT_USER_NOT_FOUND;

        } else if (userProfile.anyPosition(pos -> !pos.isEmpty())) {
            return CommandResultCode.USER_MGMT_USER_NOT_REMOVABLE_HAS_POSITIONS;

        } else if (userProfile.accounts.valuesStream().anyMatch(acc -> acc != 0)) {
            return CommandResultCode.USER_MGMT_USER_NOT_REMOVABLE_NON_EMPTY_ACCOUNTS;

        } else {
            userProfiles.remove(uidHi, uidLo);
            log.debug("Removed user profile: {}", userProfile);
            // TODO pool UserProfile objects
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

        // the profile carries its UUID
        bytes.writeInt(Math.toIntExact(userProfiles.size()));
        userProfiles.forEach((hi, lo, profile) -> profile.writeMarshallable(bytes));
    }

    @Override
    public int stateHash() {
        return HashingUtils.stateHash(userProfiles);
    }


    public UuidObjectHashtable<UserProfile> getUserProfiles() {
        return this.userProfiles;
    }
}