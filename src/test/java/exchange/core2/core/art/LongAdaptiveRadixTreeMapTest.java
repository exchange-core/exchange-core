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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

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

        assertThat(map.get(223), is("dds"));
        assertThat(map.get(Long.MAX_VALUE), is("fn"));
        assertThat(map.get(11239837212L), is("11239837212L"));


        System.out.println(map.printDiagram());

    }


    @Test
    public void shouldExtendTo16andReduceTo4() {
        LongAdaptiveRadixTreeMap<String> map = new LongAdaptiveRadixTreeMap<>();
        map.put(2, "2");
        map.put(223, "223");
        map.put(49, "49");
        map.put(1, "1");
        map.validateInternalState();
        // 4->16
        map.put(77, "77");
        map.put(4, "4");
        map.validateInternalState();

        map.remove(223);
        map.remove(1);
        // 16->4
        map.remove(4);
        map.validateInternalState();
        map.remove(49);
        map.validateInternalState();


        map.put(65536, "65536");
        map.put(131072, "131072");
        map.put(262144, "262144");
        map.validateInternalState();
        System.out.println(map.printDiagram());
        // 4->16
        map.put(524288, "524288");
        map.validateInternalState();
        System.out.println(map.printDiagram());

    }


}