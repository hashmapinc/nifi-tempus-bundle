package com.hashmapinc.tempus.nifi.processors;

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

import java.util.*;

/**
 * @author Mitesh Rathore
 *
 */

@Tags({"Json","ThingsBoard", "Attributes"})
@CapabilityDescription("Read a Json file and convert to ThingsBoard Device Attributes Json format.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class AttributesToThingsBoardDevice extends AbstractProcessor {



    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor DEVICE_NAME = new PropertyDescriptor
            .Builder().name("Device Name")
            .displayName("Device Name")
            .description("This property will direct the processor to output the Thingsboard Device name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATTRIBUTES = new PropertyDescriptor
            .Builder().name("Attributes")
            .displayName("Attributes")
            .description("This property will direct the processor to output the Thingsboard attributes. " +
                    "This will be comma separated values and must be part of the attribute list as input. eg: nameWell,nameWellbore,nameRig")
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
        descriptors.add(ATTRIBUTES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
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
        // TODO implement

        // Get the properties
        String deviceNameProperty = context.getProperty(DEVICE_NAME).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String attributeList = context.getProperty(ATTRIBUTES).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");

        StringTokenizer attributeTokenizer = new StringTokenizer(attributeList,",");

        Map<String,String> attributeKeyValue = new HashMap<>();

        while(attributeTokenizer.hasMoreTokens()){
            String attributeName = attributeTokenizer.nextToken();
            String attributeValue = flowFile.getAttribute(attributeName);
            attributeKeyValue.put(attributeName,attributeValue);
        }


        String deviceName = flowFile.getAttribute(deviceNameProperty);
        if(deviceName != null || !"".equals(deviceName)){
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

            final String outData = rootNode.toString();
            flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
            session.transfer(flowFile, SUCCESS);

        }else{

            log.error("Please correct Device Name. It should be part of input attributes: "+deviceName);
            session.transfer(flowFile, FAILURE);

        }



    }
}
