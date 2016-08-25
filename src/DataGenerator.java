/*
  Copyright Microsoft Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

import java.util.UUID;

/**
 * Random data generator methods.
 */
class DataGenerator {

    /**
     * Creates and returns a randomized name based on the prefix file for use by the sample.
     *
     * @param namePrefix The prefix string to be used in generating the name.
     * @return The randomized name
     */
    static String createRandomName(String namePrefix) {

        return namePrefix + UUID.randomUUID().toString().replace("-", "");
    }
}
