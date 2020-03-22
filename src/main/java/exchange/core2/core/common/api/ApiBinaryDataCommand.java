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


import exchange.core2.core.common.api.binary.BinaryDataCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;


@Builder
@AllArgsConstructor
public final class ApiBinaryDataCommand extends ApiCommand {

    // transfer unique id
    // can be constant unless going to push data concurrently
    public final int transferId;

    // serializable object
    public final BinaryDataCommand data;

    @Override
    public String toString() {
        return "[BINARY_DATA tid=" + transferId + " data=" + data + "]";
    }
}
