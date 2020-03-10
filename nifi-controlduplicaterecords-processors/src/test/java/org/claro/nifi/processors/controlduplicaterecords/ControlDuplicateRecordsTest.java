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
package org.claro.nifi.processors.controlduplicaterecords;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class ControlDuplicateRecordsTest {

    private TestRunner runner;
    @Before
    public void setup() {
        runner = TestRunners.newTestRunner( ControlDuplicateRecords.class );
    }

    private void setProperty (){
        runner.setProperty(ControlDuplicateRecords.DBCP_SERVICE, "Data Base");
        runner.setProperty( ControlDuplicateRecords.RECORD_READER, "reader" );
        runner.setProperty( ControlDuplicateRecords.RECORD_WRITER, "writer" );
        runner.setProperty( ControlDuplicateRecords.TABLE, "CONTROL_DUPLICATE_RECORD" );
        runner.setProperty( ControlDuplicateRecords.GROUP, "GPRS_PO" );
        runner.setProperty( ControlDuplicateRecords.MATCH_KEY, "I_Record_Type,I_Account,E_Formatted_Date" );
        runner.setProperty( ControlDuplicateRecords.STRATEGY_MATCHING, "Modify Fields" ); //Modify Fields -- Discard Record
        //runner.setProperty( ControlDuplicateRecords.STRATEGY_MATCHING, "Discard Record" );
        runner.setProperty("/E_Rec_Code", "55");
        runner.setProperty("/E_Status", "X");
        //runner.setProperty("/O_Tap_Label", "SSS20041");
    }

    @Test
    public void TestJson() throws InitializationException, IOException {
        Servicios.ConDB(runner);
        Servicios.JsonReader( runner);
        Servicios.JsonWriter( runner );
        setProperty();
        runner.clearTransferState();
        runner.enqueue( Paths.get( "src/test/inFlowFileJson"));
        runner.run();

        final String expectedOutput = new String( Files.readAllBytes(Paths.get("src/test/expectedJson")));
        runner.getFlowFilesForRelationship( ControlDuplicateRecords.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }
    /*
    @Test
    public void TestCSV() throws InitializationException, IOException {
        Servicios.ConDB(runner);
        Servicios.CSVReader(runner);
        Servicios.CSVWriter( runner );
        setProperty();
        runner.clearTransferState();
        runner.enqueue( Paths.get( "src/test/inFlowFileCSV"));
        runner.run();

        final String expectedOutput = new String( Files.readAllBytes(Paths.get("src/test/expectedCSV")));
        runner.getFlowFilesForRelationship( ControlDuplicateRecords.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testCount() throws InitializationException, IOException {
        Servicios.ConDB(runner);
        Servicios.CSVReader(runner);
        Servicios.CSVWriter( runner );
        setProperty();
        runner.clearTransferState();
        runner.enqueue( Paths.get( "src/test/inFlowFileCSV"));
        runner.run();
        runner.assertTransferCount(ControlDuplicateRecords.REL_SUCCESS, 1);
        runner.assertTransferCount(ControlDuplicateRecords.REL_DISCARD, 1);
        runner.assertTransferCount(ControlDuplicateRecords.REL_FAILURE, 0);
        runner.assertQueueEmpty();
    }*/
}
