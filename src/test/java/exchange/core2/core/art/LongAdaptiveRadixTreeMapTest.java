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
package exchange.core2.core.art;

import org.junit.Test;

import static org.junit.Assert.assertNull;

public class LongAdaptiveRadixTreeMapTest {

    @Test
    public void test() {
        LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        map.validateInternalState();
        assertNull(map.get(0));
        map.put(2, "two");
        map.validateInternalState();
        //assertThat(map.get(2), is("two"));

        map.put(223, "dds");
        map.put(49, "fn");
        map.put(1, "fn");

        System.out.println(String.format("11239847219L = %016X", 11239847219L));
        System.out.println(String.format("1123909L = %016X", 1123909L));
        map.put(Long.MAX_VALUE, "fn");
        map.put(11239847219L, "11239847219L");
        map.put(1123909L, "1123909L");
        map.put(11239837212L, "11239837212L");
        map.put(13213, "13213");
        map.put(13423, "13423");

        System.out.println(map.printDiagram());


    }


}