/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hashmapinc.tempus.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import static org.junit.Assert.*;

public class CalculateCRCTest {

    TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(CalculateCRC.class);
    }

    @Test
    public void testValidFlowFile() {

    	final String channelUri = "Testour(d95e242b-7670-45e9-ba55-33023d014736)/Tankname(8953555a-3e65-4cfa-bfd5-c6617596f174)/TankFlowIn";
        
        // mock up some attributes...
        Map<String, String> attributes = new HashMap<>();
        attributes.put("channelUri", channelUri);
      
        testRunner.setValidateExpressionUsage(false);
        testRunner.enqueue(new ByteArrayInputStream(channelUri.getBytes()),attributes);
        testRunner.run(1);

        // All results were processed with out failure
        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(CalculateCRC.SUCCESS);
        assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        
        // Test attributes and content
        result.assertAttributeEquals("crc", "4050425037");
    }
}
