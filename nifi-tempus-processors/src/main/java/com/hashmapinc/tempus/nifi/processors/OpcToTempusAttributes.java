package com.hashmapinc.tempus.nifi.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


/**
 * Created by Shubham Gupta on 08-03-2018.
 */

@Tags({"Tempus", "OPC-UA", "Json", "Attributes"})
@CapabilityDescription("Read output flowfile from GetOPCNodeList processor and convert the data to Tempus Device/Gateway Attributes Json format.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class OpcToTempusAttributes extends AbstractProcessor {
    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor ATTRIBUTE_JSON_FORMAT = new PropertyDescriptor
            .Builder().name("Attribute Json Format")
            .displayName("Attribute Json Format")
            .description("Select the type of attribute Json you want")
            .defaultValue("Gateway Json")
            .allowableValues("Gateway Json", "Device Json")
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
        descriptors.add(ATTRIBUTE_JSON_FORMAT);
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
        ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();

        if ( flowFile == null ) {
            logger.error("Flow file is null");
            return;
        }

        // Initialize  response variable
        final AtomicReference<List<String>> opcuaListData = new AtomicReference<>();

        // Read opc data from flow file content
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {

                try{
                    List<String> tagname = new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.toList());
                    opcuaListData.set(tagname);
                }catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    logger.error("Failed to read");
                }
            }
        });

        int recursiveDepth = Integer.parseInt(flowFile.getAttribute("recursiveDepth"));
        String startingNode = flowFile.getAttribute("startNode");

        StringBuilder opcuaData = removeUnnecessaryOpcData(opcuaListData.get(), recursiveDepth);

        if (opcuaData.length() == 0) {
            logger.error("No useful data extracted");
            session.transfer(flowFile, FAILURE);
        } else {

            String nodesData[] = opcuaData.toString().split("\\r?\\n");
            JsonNode jsonNode = null;

            if (context.getProperty(ATTRIBUTE_JSON_FORMAT).getValue().equals("Gateway Json")) {
                jsonNode = transformOpcDataToGatewayAttributes(nodesData);
            } else {
                jsonNode = transformOpcDataToDeviceAttributes(nodesData);
            }

            if (jsonNode != null) {
                try {
                    final String outData = mapper.writeValueAsString(jsonNode);
                    session.putAttribute(flowFile, "mime.type", "application/json");
                    flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
                    session.transfer(flowFile, SUCCESS);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    session.transfer(flowFile, FAILURE);
                }
            } else {
                logger.error("OPCUA data generated is null");
                session.transfer(flowFile, FAILURE);
            }
        }
    }

    private JsonNode transformOpcDataToGatewayAttributes(String[]  nodesData) {
        JsonNode jsonNode = mapper.createObjectNode();
        JsonNode jsonValue;
        for (int i = 0; i < nodesData.length; i++) {
            jsonValue = mapper.createObjectNode();
            String key = extractKey(nodesData[i]);
            String value = extractValue(nodesData[i]);

            ((ObjectNode) jsonValue).put(value, false);
            while (true) {
                i++;
                if (i >= nodesData.length) {
                    break;
                }

                String keyNext = extractKey(nodesData[i]);
                if (key.equals(keyNext)) {
                    ((ObjectNode) jsonValue).put(extractValue(nodesData[i]), false);
                } else {
                    i--;
                    break;
                }
            }
            ((ObjectNode) jsonNode).set(key, jsonValue);
        }
        return jsonNode;
    }

    private JsonNode transformOpcDataToDeviceAttributes(String[]  nodesData) {
        JsonNode jsonNode = mapper.createObjectNode();
        String key = extractKey(nodesData[0]);
        for (int i = 0; i < nodesData.length; i++) {
            String value = extractValue(nodesData[i]);
            ((ObjectNode) jsonNode).put(value, false);
        }
        return jsonNode;
    }

    private StringBuilder removeUnnecessaryOpcData(List<String> opcuaListData, int recursiveDepth) {
        StringBuilder opcuaData = new StringBuilder();
        for (int i = 0; i < opcuaListData.size(); i++) {
            if (opcuaListData.get(i).contains("nsu")) {
                continue;
            }
            String nodes[] = opcuaListData.get(i).split("\\.");
            if (nodes.length < recursiveDepth) {
                continue;
            }
            if (nodes[nodes.length - 1].startsWith("_")) {
                continue;
            }
            opcuaData.append(opcuaListData.get(i) + System.getProperty("line.separator"));
        }
        return opcuaData;
    }

    private String extractKey(String nodeData) {
        String keyValue[] = nodeData.split("\\.");
        String key = "";
        for (int j = 0; j < keyValue.length-1; j++) {
            key = key + keyValue[j].replace("- ", "") + ".";
        }
        key = key.substring(0, key.length() - 1);
        return key;
    }

    private String extractValue(String nodeData) {
        String keyValue[] = nodeData.split("\\.");
        return keyValue[keyValue.length - 1];
    }
}