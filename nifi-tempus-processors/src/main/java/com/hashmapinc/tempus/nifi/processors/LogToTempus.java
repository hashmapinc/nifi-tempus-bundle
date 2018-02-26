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
 * @author Vilas Deshmukh
 *
 */

@Tags({"Tempus", "Json", "Telemetry", "Log"})
@CapabilityDescription("Read a WITSML log object and convert to ThingsBoard Device Telemetry Json format.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class LogToTempus extends AbstractProcessor {

    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor DEVICE_NAME = new PropertyDescriptor
            .Builder().name("Device Name")
            .displayName("Device Name")
            .description("This property will direct the processor to output the Thingsboard Device name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOG_NAME = new PropertyDescriptor
            .Builder().name("Log Name")
            .displayName("Log Name")
            .description("This property will direct the processor to construct key for the telemetry.")
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

    public static final PropertyDescriptor LOG_SOURCEFIELD = new PropertyDescriptor
            .Builder().name("Log Source Fieldname")
            .displayName("Log Source Fieldname")
            .description("This property will direct the processor to output the Thingsboard Telemetry for Time or Depth data.")
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
        descriptors.add(LOG_NAME);
        descriptors.add(LOG_TYPE);
        descriptors.add(LOG_SOURCEFIELD);
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
            log.warn("Flow file is null");
            return;
        }

        // Get the properties
        String deviceNameProperty = context.getProperty(DEVICE_NAME).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String logNameProperty = context.getProperty(LOG_NAME).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String logType = context.getProperty(LOG_TYPE).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String wmlObjectType = flowFile.getAttribute("wmlObjectType");
        String logSourceField = context.getProperty(LOG_SOURCEFIELD).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");

        String deviceName = flowFile.getAttribute(deviceNameProperty);
        boolean isMessage=false;
        if (wmlObjectType.equalsIgnoreCase("message")) {
            isMessage = true;
            getLogger().debug("a message object");
        } else {
            getLogger().debug("not a message object");
        }
        String logName = flowFile.getAttribute(logNameProperty);
        String[] mnemonicValueList = null;
        String[] values = null;

        if(deviceName != null || !deviceName.isEmpty()){
            ObjectNode logResponse = mapper.createObjectNode();
            ArrayNode logData = logResponse.putArray(deviceName);
            ObjectNode logInstance = logData.addObject();
            JsonNode json=getJsonObject(session, flowFile);
            if(NifiJsonUtil.TS_FIELDNAME.equalsIgnoreCase(logType)){
                logInstance.put(logType,String.valueOf(NifiJsonUtil.changeTimeToLong(json.get(logSourceField).textValue())));
            } else {
                logInstance.put(logType,String.valueOf(json.get(logSourceField).textValue()));
            }
            ObjectNode kvData=mapper.createObjectNode();

            mnemonicValueList = getFieldList(json, logSourceField);
            for (String mnemonic : mnemonicValueList) {
                if (isMessage)
                    kvData.put(logNameProperty, json.get(mnemonic).textValue());
                else
                    kvData.put(mnemonic+"@"+logName, json.get(mnemonic).textValue());
            }
            logInstance.put("values", kvData);
            String outData="";
            try {
                outData = mapper.writeValueAsString(logResponse);
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

    private String[] getFieldList(JsonNode json, String dsts) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> fields = json.fieldNames();
        boolean fieldsAdded=false;
        while (fields.hasNext()) {
            String field=fields.next();
            if (field.equalsIgnoreCase(dsts))
                continue;
            if (fieldsAdded)
                sb.append(",");
            sb.append(field);
            fieldsAdded=true;
        }
        return sb.toString().split(",");
    }

    private JsonNode getJsonObject(final ProcessSession session, FlowFile flowFile) {
        InputStream inputStream = session.read(flowFile);
        String flowFileContents=null;
        JsonNode jsonRequest=null;
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))){
            flowFileContents = bufferedReader.lines().collect(Collectors.joining("\n"));
            ObjectMapper mapper = new ObjectMapper();
            jsonRequest = mapper.readTree(flowFileContents);
        }catch(IOException ex){
            getLogger().error("Error reading flow file contents "+ex.getMessage());
            session.transfer(flowFile, FAILURE);
        }
        return jsonRequest;
    }
}

