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

public final class ApiSuspendUser extends ApiCommand {

    public final long uid;

    @Override
    public String toString() {
        return "[SUSPEND_USER " + uid + "]";
    }

    ApiSuspendUser(long uid) {
        this.uid = uid;
    }

    public static ApiSuspendUserBuilder builder() {
        return new ApiSuspendUserBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiSuspendUser other
                && other.canEqual(this)
                && this.uid == other.uid;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiSuspendUser;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.uid);
        return result;
    }

    public static class ApiSuspendUserBuilder {
        private long uid;

        ApiSuspendUserBuilder() {
        }

        public ApiSuspendUserBuilder uid(long uid) {
            this.uid = uid;
            return this;
        }

        public ApiSuspendUser build() {
            return new ApiSuspendUser(this.uid);
        }

        @Override
        public String toString() {
            return "ApiSuspendUser.ApiSuspendUserBuilder(uid=" + this.uid + ")";
        }
    }
}
