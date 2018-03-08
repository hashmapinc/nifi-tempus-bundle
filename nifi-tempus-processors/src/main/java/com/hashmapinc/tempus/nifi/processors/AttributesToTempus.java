package com.hashmapinc.tempus.nifi.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.util.StringUtils;

import java.util.*;

/**
 * @author Mitesh Rathore
 *
 */

@Tags({"Tempus","Json", "Attributes"})
@CapabilityDescription("Read a Json file and convert to Tempus Device Attributes Json format.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class AttributesToTempus extends AbstractProcessor {



    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor DEVICE_NAME = new PropertyDescriptor
            .Builder().name("Device Name")
            .displayName("Device Name")
            .description("This property will direct the processor to output the Tempus Device name.")
            .defaultValue("nameWell")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATTRIBUTES = new PropertyDescriptor
            .Builder().name("Attributes")
            .displayName("Attributes")
            .description("This property will direct the processor to output the Tempus attributes. " +
                    "This will be comma separated values and must be part of the attribute list as input. eg: nameWell,nameWellbore,nameRig")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEVICE_TYPE = new PropertyDescriptor
            .Builder().name("Device Type")
            .displayName("Device Type")
            .description("This property will be used to display device type on Tempus UI and should be used only when publishing to \"connect\" topic.")
            .required(false)
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
        descriptors.add(ATTRIBUTES);
        descriptors.add(DEVICE_TYPE);
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
        // TimeLogObject timeLogObject = null;

        if ( flowFile == null ) {
            log.error("Flow file is null");
            return;
        }

        // Get the properties
        String deviceNameProperty = context.getProperty(DEVICE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if(!StringUtils.isEmpty(deviceNameProperty)){
            deviceNameProperty.replaceAll("[;\\s\t]", "");
        }
        String attributeList = context.getProperty(ATTRIBUTES).evaluateAttributeExpressions(flowFile).getValue();

        Map<String,String> attributeKeyValue = new HashMap<>();
        if(!StringUtils.isEmpty(attributeList)){
            attributeList.replaceAll("[;\\s\t]", "");
            StringTokenizer attributeTokenizer = new StringTokenizer(attributeList,",");
            while(attributeTokenizer.hasMoreTokens()){
                String attributeName = attributeTokenizer.nextToken();
                String attributeValue = flowFile.getAttribute(attributeName);
                if(!StringUtils.isEmpty(attributeValue))
                    attributeKeyValue.put(attributeName,attributeValue);
            }
        }

        String deviceTypeProperty = context.getProperty(DEVICE_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        if(!StringUtils.isEmpty(deviceTypeProperty)){
            deviceTypeProperty.replaceAll("[;\\s\t]", "");
        }

        String deviceName = flowFile.getAttribute(deviceNameProperty);
        if(!StringUtils.isEmpty(deviceName) && !StringUtils.isEmpty(attributeList) && StringUtils.isEmpty(deviceTypeProperty)){
            ObjectNode attributeNodes = mapper.createObjectNode();
            List<ObjectNode> attributeNodeList = new ArrayList<>();
            attributeKeyValue.forEach((attributeName,attributeValue) ->{
                attributeNodes.put(attributeName, attributeValue);
                attributeNodeList.add(attributeNodes);
            });

            ArrayNode array = mapper.valueToTree(attributeNodeList);
            ObjectNode rootNode = mapper.createObjectNode();
            attributeNodeList.forEach(attrNode ->{
                rootNode.putPOJO(deviceName,attrNode);
            });

            try {

                final String outData = mapper.writeValueAsString(rootNode);
                flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
                session.transfer(flowFile, SUCCESS);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                session.transfer(flowFile, FAILURE);
            }

        }else if(!StringUtils.isEmpty(deviceName) && !StringUtils.isEmpty(deviceTypeProperty) && StringUtils.isEmpty(attributeList)){

            ObjectNode connectNode = mapper.createObjectNode();
            connectNode.put("device",deviceName);
            connectNode.put("type",deviceTypeProperty);
            try {
                final String outData = mapper.writeValueAsString(connectNode);
                flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
                session.transfer(flowFile, SUCCESS);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                session.transfer(flowFile, FAILURE);
            }

        }

        else{

            log.error("Please correct Device Name. It should be part of input attributes: "+deviceName);
            session.transfer(flowFile, FAILURE);

        }


    }
}
