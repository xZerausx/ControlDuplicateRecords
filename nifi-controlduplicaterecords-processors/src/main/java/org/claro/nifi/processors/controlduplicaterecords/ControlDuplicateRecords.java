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

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.redis.RedisConnectionPool;

import java.io.OutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


@Tags({"mediación", "claro", "record", "dba","control","duplicate","Zeraus","Ivan"})
@CapabilityDescription("")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ControlDuplicateRecords extends AbstractProcessor {

    static final String MODIFY = "Modify Fields";
    static final String DISCARD = "Discard Record";
    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .identifiesControllerService(RedisConnectionPool.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name( "Reader" )
            .displayName( "Record Reader" )
            .identifiesControllerService( RecordReaderFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name( "Writer" )
            .displayName( "Record Writer" )
            .identifiesControllerService( RecordSetWriterFactory.class )
            .required( true )
            .build();
    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name( "Table" )
            .displayName( "Table" )
            .description( "Registration table name" )
            .required( true )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .build();
    static final PropertyDescriptor GROUP = new PropertyDescriptor.Builder()
            .name( "group" )
            .displayName( "Group" )
            .description( "registration group name" )
            .required( true )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .build();
    static final PropertyDescriptor MATCH_KEY = new PropertyDescriptor.Builder()
            .name( "match key" )
            .displayName( "Match Key" )
            .description( "fields to match key" )
            .required( true )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .build();
    static final PropertyDescriptor STRATEGY_MATCHING = new PropertyDescriptor.Builder()
            .name( "strategy matching" )
            .displayName( "Strategy Matching" )
            .description( "The strategy match has 2 modes: \"modify fields\" or \"discard record\"" )
            .required( true )
            .allowableValues(MODIFY,DISCARD)
            .defaultValue( DISCARD )
            .addValidator( StandardValidators.NON_EMPTY_VALIDATOR )
            .build();
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
            return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("Specifies the value to use to replace fields in the record that match the RecordPath: " + propertyDescriptorName)
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new RecordPathPropertyNameValidator())
            .build();
    }

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name( "success" )
            .build();
    static final Relationship REL_DISCARD = new Relationship.Builder()
            .name( "discard" )
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name( "failure" )
            .build();

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
    }

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add( DBCP_SERVICE );
        descriptors.add( RECORD_READER );
        descriptors.add( RECORD_WRITER );
        descriptors.add( TABLE );
        descriptors.add( GROUP );
        descriptors.add( MATCH_KEY );
        descriptors.add( STRATEGY_MATCHING );
        this.descriptors = Collections.unmodifiableList( descriptors );

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add( REL_SUCCESS );
        relationships.add( REL_DISCARD );
        relationships.add( REL_FAILURE );
        this.relationships = Collections.unmodifiableSet( relationships );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    private DBCPService dbcpService;

    @OnScheduled
    public void setup(ProcessContext context) {
        dbcpService =  context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final Connection con = dbcpService.getConnection( flowFile.getAttributes() );

        /*
        if (context.getProperty(DBCP_SERVICE).isSet()){
             con = dbcpService.getConnection( flowFile.getAttributes() );
        } else if(context.getProperty(REDIS_CONNECTION_POOL).isSet()) {
          // con = RedisConnectionPool.getConnection( flowFile.getAttributes() );
        } else{
            return;
        }*/

        final RecordReaderFactory factory = context.getProperty( RECORD_READER ).asControllerService( RecordReaderFactory.class );
        final RecordSetWriterFactory writerFactory = context.getProperty( RECORD_WRITER ).asControllerService( RecordSetWriterFactory.class );

        final String table = context.getProperty( TABLE ).getValue();
        final String group = context.getProperty( GROUP ).getValue();
        final String matchKey = context.getProperty( MATCH_KEY ).getValue();
        final String stretegyMatching = context.getProperty( STRATEGY_MATCHING ).getValue();

        final ComponentLog logger = getLogger();

        final FlowFile outFlowFile = session.create( flowFile );
        final FlowFile outFlowFileDiscard = session.create( flowFile );

        try {
            session.read( flowFile, in -> {
                try (RecordReader reader = factory.createRecordReader( flowFile, in, logger )) {
                    final RecordSchema writeSchema = writerFactory.getSchema( null, reader.getSchema() );

                    Record record;
                    final OutputStream outStream = session.write( outFlowFile );
                    final RecordSetWriter writer= writerFactory.createWriter( getLogger(), writeSchema, outStream );
                    final RecordSetWriter writerDiscard= writerFactory.createWriter( getLogger(), writeSchema, outStream );
                    writer.beginRecordSet();
                    writerDiscard.beginRecordSet();
                    String[] splitMatchKey = matchKey.split( "," );
                    String sql = "insert into " + table + " (cdr_group, cdr_key) values (?, ?)";
                    while ((record = reader.nextRecord()) != null) {
                        //########################################################
                        String hash = generateHash (splitMatchKey,logger, record);

                        boolean duplicate = !insertHash(con, sql, group, hash , logger);
                        logger.debug( "# Duplicado?  " + duplicate);

                        if(duplicate){
                            if (stretegyMatching.equals( DISCARD )){
                                writerDiscard.write( record );
                                logger.debug( "# DISCARD");
                            } else {
                                logger.debug( "# MODIFY");
                                modifyRecord (context,  record,  flowFile, logger);
                                writer.write( record );
                            }
                        }else {
                            writer.write( record );
                        }
                        //######################################################
                    }
                    final WriteResult writeResult = writer.finishRecordSet();
                    final WriteResult writeResultDiscard = writerDiscard.finishRecordSet();
                    writer.close();
                    writerDiscard.close();

                    if(writeResult.getRecordCount()>0){
                        final Map<String, String> attributes = new HashMap<>();
                        attributes.put( "record.count", String.valueOf( writeResult.getRecordCount() ) );
                        attributes.put( CoreAttributes.MIME_TYPE.key(), writer.getMimeType() );
                        session.putAllAttributes( outFlowFile, attributes );
                        session.transfer( outFlowFile, REL_SUCCESS );
                    }else{
                        session.remove( outFlowFile );
                    }

                    if (writeResultDiscard.getRecordCount()>0){
                        final Map<String, String> attributesDiscard = new HashMap<>();
                        attributesDiscard.put( "record.count", String.valueOf( writeResultDiscard.getRecordCount() ) );
                        attributesDiscard.put( CoreAttributes.MIME_TYPE.key(), writerDiscard.getMimeType() );
                        session.putAllAttributes( outFlowFileDiscard, attributesDiscard );
                        session.transfer( outFlowFileDiscard, REL_DISCARD );
                    }else {
                        session.remove( outFlowFileDiscard );
                    }

                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    getLogger().error( "SchemaNotFound or MalformedRecordException \n" + e.toString() );
                    throw new ProcessException( e );
                } catch (final Exception e) {
                    getLogger().error( e.toString() );
                    throw new ProcessException( e );
                }
            } );
        } catch (final Exception e) {
            session.transfer( flowFile, REL_FAILURE );
            logger.error( "Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e} );
            return;
        }
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        session.remove( flowFile );
    }

    private void modifyRecord(ProcessContext context, Record record, FlowFile flowFile, ComponentLog logger) {
        // CONSULTAR A IVAN
        for (final String recordPathText : recordPaths) {
            final PropertyValue replacementValue = context.getProperty( recordPathText );
            final RecordPath recordPath = recordPathCache.getCompiled( recordPathText );
            final RecordPathResult result = recordPath.evaluate( record );
            final String evaluatedReplacementVal = replacementValue.evaluateAttributeExpressions( flowFile ).getValue();
            logger.debug( "# Replace field: " + recordPath + "  Value: "  + evaluatedReplacementVal );
            result.getSelectedFields().forEach( fieldVal -> fieldVal.updateValue( evaluatedReplacementVal, RecordFieldType.STRING.getDataType() ) );
        }
    }

    private boolean insertHash(final Connection con,String sql,String group, String key,ComponentLog logger) throws SQLException {
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1,group);
        ps.setString(2,key);
        try {
            ps.executeQuery();
            logger.debug( "### SQL INSERT OK");
        } catch (SQLException exception) {
            logger.debug( "### SQL ERRROR: " + exception.getErrorCode());
            if(exception.getErrorCode()== 1) {
                return false;
            }else {
                throw new ProcessException( exception );
            }
            // PREGUNTAR QUE SE HACE SI DEVUELVE CUALQUIER OTRO ERROR
        }
        return true;
    }
 /*    public  String generateHash(String[] splitMatchKey,ComponentLog logger, Record record) throws NoSuchAlgorithmException {
        String testString = "";
        for (String s : splitMatchKey) {
            testString = testString + record.getValue( s );
        }
        logger.debug("# testString:  " + testString);
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] messageDigest = md.digest(testString.getBytes());
        BigInteger number = new BigInteger(1, messageDigest);
        String hashtext = number.toString(16);
        logger.debug("# MD5:  " + hashtext);
        return hashtext;
    }*/
    public  String generateHash(String[] splitMatchKey,ComponentLog logger, Record record) {
        String testString = "";
        for (String s : splitMatchKey) {
            testString = testString + "|" + record.getValue( s );
        }
        testString = testString + "|";
        logger.debug("# testString:  " + testString);
        return testString;
    }
}

