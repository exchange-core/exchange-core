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
package exchange.core2.core.common.api.binary;

import exchange.core2.core.utils.SerializationUtils;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public final class BatchAddAccountsCommand implements BinaryDataCommand {

    // user UUID -> currency -> balance, in the order users are added
    private final LinkedHashMap<UUID, IntLongHashMap> users;

    public BatchAddAccountsCommand(final BytesIn bytes) {
        final int size = bytes.readInt();
        users = new LinkedHashMap<>(Math.max(16, size * 4 / 3 + 1));
        for (int i = 0; i < size; i++) {
            final UUID uid = new UUID(bytes.readLong(), bytes.readLong());
            users.put(uid, SerializationUtils.readIntLongHashMap(bytes));
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(users.size());
        users.forEach((uid, accounts) -> {
            bytes.writeLong(uid.getMostSignificantBits());
            bytes.writeLong(uid.getLeastSignificantBits());
            SerializationUtils.marshallIntLongHashMap(accounts, bytes);
        });
    }

    @Override
    public int getBinaryCommandTypeCode() {
        return BinaryCommandType.ADD_ACCOUNTS.getCode();
    }

    public BatchAddAccountsCommand(Map<UUID, IntLongHashMap> users) {
        this.users = new LinkedHashMap<>(users);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof BatchAddAccountsCommand other
                && Objects.equals(this.getUsers(), other.getUsers());
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        var users = this.getUsers();
        result = result * prime + (users == null ? 43 : users.hashCode());
        return result;
    }

    public Map<UUID, IntLongHashMap> getUsers() {
        return this.users;
    }
}
