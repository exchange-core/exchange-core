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

public final class ApiAddUser extends ApiCommand {

    public final long uid;

    @Override
    public String toString() {
        return "[ADDUSER " + uid + "]";
    }

    public ApiAddUser(long uid) {
        this.uid = uid;
    }

    public static ApiAddUserBuilder builder() {
        return new ApiAddUserBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiAddUser other
                && other.canEqual(this)
                && this.uid == other.uid;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiAddUser;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.uid);
        return result;
    }

    public static class ApiAddUserBuilder {
        private long uid;

        ApiAddUserBuilder() {
        }

        public ApiAddUserBuilder uid(long uid) {
            this.uid = uid;
            return this;
        }

        public ApiAddUser build() {
            return new ApiAddUser(this.uid);
        }

        @Override
        public String toString() {
            return "ApiAddUser.ApiAddUserBuilder(uid=" + this.uid + ")";
        }
    }
}
