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
package exchange.core2.core.common.api;

public final class ApiRemoveUser extends ApiCommand {

    public final long uid;

    @Override
    public String toString() {
        return "[REMOVE_USER " + uid + "]";
    }

    ApiRemoveUser(long uid) {
        this.uid = uid;
    }

    public static ApiRemoveUserBuilder builder() {
        return new ApiRemoveUserBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiRemoveUser other
                && other.canEqual(this)
                && this.uid == other.uid;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiRemoveUser;
    }

    @Override
    public int hashCode() {
        return 59 + Long.hashCode(uid);
    }

    public static class ApiRemoveUserBuilder {
        private long uid;

        ApiRemoveUserBuilder() {
        }

        public ApiRemoveUserBuilder uid(long uid) {
            this.uid = uid;
            return this;
        }

        public ApiRemoveUser build() {
            return new ApiRemoveUser(uid);
        }

        @Override
        public String toString() {
            return "ApiRemoveUser.ApiRemoveUserBuilder(uid=" + uid + ")";
        }
    }
}
