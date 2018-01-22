package com.hashmapinc.tempus.nifi.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hashmapinc.tempus.nifi.util.NifiJsonUtil;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Mitesh Rathore
 *
 */

@Tags({"Json","ThingsBoard", "Telemetry"})
@CapabilityDescription("Read a Json file and convert to ThingsBoard Device Telemetry Json format.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class TelemetryToThingsBoardDevice extends AbstractProcessor {

    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor DEVICE_NAME = new PropertyDescriptor
            .Builder().name("Device Name")
            .displayName("Device Name")
            .description("This property will direct the processor to output the Thingsboard Device name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOG_TYPE = new PropertyDescriptor
            .Builder().name("Log Type")
            .displayName("Log Type")
            .description("This property will direct the processor to output the Thingsboard Telemetry for Time or Depth data.")
            .required(true)
            .allowableValues("ts","ds")
            .defaultValue("ts")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCEFIELD_NAME = new PropertyDescriptor
            .Builder().name("Source Field Name")
            .displayName("Source Field Name")
            .description("This property will direct the processor to output the Thingsboard Telemetry for Time or Depth data. eg:index ")
            .required(true)
            .defaultValue("index")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_CONSTRUCT = new PropertyDescriptor
            .Builder().name("Key Construct")
            .displayName("Key Construct")
            .description("This property will direct the processor to output the Thingsboard Telemetry for Time or Depth data for these values." +
                    "Values must be comma separated. eg:mnemonic,/,logName")
            .required(true)
            .defaultValue("mnemonic,/,logName")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALUE_FIELD = new PropertyDescriptor
            .Builder().name("Value field")
            .displayName("Value field")
            .description("This property will direct the processor to output the Thingsboard Telemetry for Time or Depth data. ")
            .defaultValue("value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successfully transferred attributes.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DEVICE_NAME);
        descriptors.add(LOG_TYPE);
        descriptors.add(SOURCEFIELD_NAME);
        descriptors.add(KEY_CONSTRUCT);
        descriptors.add(VALUE_FIELD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);

        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ComponentLog log = getLogger();
        FlowFile flowFile = session.get();
       // LogObject logObject = null;

        if ( flowFile == null ) {
            log.error("Flow file is null");
            return;
        }

        // Get the properties
        String deviceNameProperty = context.getProperty(DEVICE_NAME).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String logType = context.getProperty(LOG_TYPE).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String sourceFieldName = context.getProperty(SOURCEFIELD_NAME).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String keyConstruct = context.getProperty(KEY_CONSTRUCT).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String valueFieldName = context.getProperty(VALUE_FIELD).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");

        String deviceName = flowFile.getAttribute(deviceNameProperty);
        String flowFileContents = null;
        JsonNode jsonRequest = null;
        String timeDepthValue = null;
        String indexValue = null;

        if(deviceName != null || !deviceName.isEmpty()){

            InputStream inputStream = session.read(flowFile);
            try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))){
                flowFileContents = bufferedReader.lines().collect(Collectors.joining("\n"));
                jsonRequest = mapper.readTree(flowFileContents);
                timeDepthValue = jsonRequest.get(sourceFieldName).textValue();//logObject.getIndex();
                indexValue = jsonRequest.get(valueFieldName).textValue();

            }catch(IOException ex){
                log.error("Error reading flow file contents "+ex.getMessage());
                session.transfer(flowFile, FAILURE);
            }
            ObjectNode logResponse = mapper.createObjectNode();
            String outData = null;
            if(NifiJsonUtil.TS_FIELDNAME.equalsIgnoreCase(logType)){
                long longTime = NifiJsonUtil.changeTimeToLong(timeDepthValue);
                logResponse.put(logType,String.valueOf(longTime));
            }else{
                logResponse.put(logType,timeDepthValue);
           }

            StringTokenizer attributeTokenizer = new StringTokenizer(keyConstruct,NifiJsonUtil.KEY_CONSTRUCT_DELIMETER);

            StringBuffer valueFiledBuffer = new StringBuffer();

            while(attributeTokenizer.hasMoreTokens()){
                String attributeName = attributeTokenizer.nextToken();
                try{
                    String jsonValue = jsonRequest.get(attributeName).textValue();
                    valueFiledBuffer.append(jsonValue);
                }catch(NullPointerException exp){
                    log.info("Can not retrieve the values for attribute "+attributeName + " -> ");
                    valueFiledBuffer.append(attributeName);
                }
                catch(Exception exp){
                    log.info("Can not retrieve the values for attribute "+attributeName + " -> "+exp.getMessage());
                    valueFiledBuffer.append(attributeName);
                }
            }

            ObjectNode valueNode = mapper.createObjectNode();
            valueNode.put(valueFiledBuffer.toString(), indexValue);

            logResponse.putPOJO(NifiJsonUtil.LOG_CHILD_ELEMENT,valueNode);

            List<ObjectNode> timeLogObjectList = new ArrayList<>();
            timeLogObjectList.add(logResponse);
            ArrayNode array = mapper.valueToTree(timeLogObjectList);
            ObjectNode logParentNode = mapper.createObjectNode();
            logParentNode.putArray(deviceName).addAll(array);

            try {
                outData = mapper.writeValueAsString(logParentNode);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            final String returnData = outData;

            flowFile = session.write(flowFile, out -> out.write(returnData.getBytes()));
            session.transfer(flowFile, SUCCESS);

        }else{

            log.error("Please correct Device Name. It should be part of input attributes: "+deviceName);
            session.transfer(flowFile, FAILURE);

        }



    }
}
