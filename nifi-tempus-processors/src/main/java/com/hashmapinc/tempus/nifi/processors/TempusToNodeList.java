package com.hashmapinc.tempus.nifi.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Shubham Gupta on 14-03-2018.
 */

@Tags({"Tempus", "OPC-UA", "Node-List", "Attributes"})
@CapabilityDescription("Read the json data with attributes names and values. Convert the Json data to OPC node list which will be input GetOpcData.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class TempusToNodeList extends AbstractProcessor {
    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor STORAGE_FILE_NAME = new PropertyDescriptor
            .Builder().name("File Path")
            .displayName("File Path")
            .description("Provide the path of file where the List of Nodes is stored.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_TYPE = new PropertyDescriptor
            .Builder().name("Data Type")
            .displayName("Data Type")
            .description("Select one of the option, to specify the type of data.")
            .allowableValues("OPC", "WITSML")
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
        descriptors.add(STORAGE_FILE_NAME);
        descriptors.add(DATA_TYPE);
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
        final AtomicReference<String> flowfileData = new AtomicReference<>();

        // Read opc data from flow file content
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {

                try{
                    String tagname = new BufferedReader(new InputStreamReader(in)).readLine();
                    flowfileData.set(tagname);
                }catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    logger.error("Failed to read");
                }
            }
        });

        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(flowfileData.get());
        } catch (IOException ex) {
            logger.error("Error in parsing JSON string");
            return;
        }

        String dataType = context.getProperty(DATA_TYPE).getValue();
        File filePath = new File(context.getProperty(STORAGE_FILE_NAME).getValue());

        session.putAttribute(flowFile, "filename", filePath.getName());
        if (dataType.equalsIgnoreCase("OPC")) {
            // Read OPC Nodes file content and store in HashSet
            HashSet<String> opcNodeList;
            opcNodeList = readOpcStorageFile(filePath);

            Iterator<Map.Entry<String, JsonNode>> attributes = jsonNode.fields();
            while (attributes.hasNext()) {
                Map.Entry<String, JsonNode> attribute = attributes.next();
                processOpcAttribute(attribute, opcNodeList);
            }

            if (!opcNodeList.isEmpty()) {
                final String outData = parseOpcSetData(opcNodeList);
                flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
                session.transfer(flowFile, SUCCESS);
            } else {
                logger.error("OPCUA Set is empty");
                session.transfer(flowFile, FAILURE);
            }
        } else {
            // Read Witsml Objects file content
            JsonNode witsmlJsonData = readWitsmlStorageFile(filePath);

            Iterator<Map.Entry<String, JsonNode>> attributes = jsonNode.fields();
            while (attributes.hasNext()) {
                Map.Entry<String, JsonNode> attribute = attributes.next();
                processWitsmlAttribute(attribute, witsmlJsonData);
            }

            final String outData = witsmlJsonData.toString();
            flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
            session.transfer(flowFile, SUCCESS);
        }
    }

    private HashSet<String> readOpcStorageFile(File filePath) {
        HashSet<String> opcNodeList = new HashSet<>();
        try(BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String currentLine;
            while ((currentLine = br.readLine()) != null) {
                opcNodeList.add(currentLine);
            }
        } catch (IOException ex) {
            getLogger().info("Error in reading storage file or file doesn't exist");
        }
        return opcNodeList;
    }

    private void processOpcAttribute(Map.Entry<String, JsonNode> attribute, HashSet<String> opcNodeList) {
        if (attribute.getKey().equals("deleted")) {
            removeOpcDeletedAttributes(attribute.getValue(), opcNodeList);
        } else if (!attribute.getValue().asBoolean()) {
            opcNodeList.remove(attribute.getKey());
        } else {
            opcNodeList.add(attribute.getKey());
        }
    }

    private void removeOpcDeletedAttributes(JsonNode attrList, HashSet<String> opcNodeList) {
        Iterator<JsonNode> iterator = attrList.elements();
        while (iterator.hasNext()) {
            JsonNode js = iterator.next();
            opcNodeList.remove(js.textValue());
        }
    }

    private String parseOpcSetData(HashSet<String> opcNodeList) {
        StringBuilder nodeListData = new StringBuilder();
        Iterator<String> it = opcNodeList.iterator();
        while (it.hasNext()) {
            nodeListData.append(it.next() + System.getProperty("line.separator"));
        }
        return nodeListData.toString();
    }
    
    private JsonNode readWitsmlStorageFile(File filePath) {
        JsonNode witsmlJsonData;
        try {
            witsmlJsonData = mapper.readTree(filePath);
        } catch (IOException ex) {
            witsmlJsonData = mapper.createObjectNode();
            getLogger().info("Error in reading storage file or file doesn't exist");
        }
        return witsmlJsonData;
    }

    private void processWitsmlAttribute(Map.Entry<String, JsonNode> attribute, JsonNode witsmlJson) {
        String key = attribute.getKey();
        if (key.equals("deleted")) {
            removeWitsmlDeletedAttributes(attribute.getValue(), witsmlJson);
        } else if (!attribute.getValue().asBoolean()) {
            String[] kvData = key.split("@");
            deleteKvDataFromJson(kvData, witsmlJson);
        } else {
            String[] kvData = key.split("@");
            addKvDataToJson(kvData, witsmlJson);
        }
    }

    private void removeWitsmlDeletedAttributes(JsonNode attrList, JsonNode witsmlJson) {
        Iterator<JsonNode> iterator = attrList.elements();
        while (iterator.hasNext()) {
            JsonNode js = iterator.next();
            String[] kvData = js.asText().split("@");
            deleteKvDataFromJson(kvData, witsmlJson);
        }
    }

    private void deleteKvDataFromJson(String[] kvData, JsonNode witsmlJson) {
        String value = witsmlJson.findValue(kvData[0]).asText();
        List<String> list = new LinkedList<String>(Arrays.asList(value.split(",")));
        if (list.contains(kvData[1])) {
            list.remove(kvData[1]);
            if (list.size() == 0) {
                ((ObjectNode) witsmlJson).remove(kvData[0]);
                return;
            }
        }
        String mnemonicStr = convertListToString(list);
        ((ObjectNode) witsmlJson).put(kvData[0], mnemonicStr);
    }

    private void addKvDataToJson(String[] kvData, JsonNode witsmlJson) {
        if (witsmlJson.has(kvData[0])) {
            String value = witsmlJson.findValue(kvData[0]).asText();
            List<String> list = new LinkedList<String>(Arrays.asList(value.split(",")));
            list.add(kvData[1]);
            value = convertListToString(list);
            ((ObjectNode)witsmlJson).put(kvData[0], value);
        } else {
            ((ObjectNode)witsmlJson).put(kvData[0], kvData[1]);
        }
    }

    private String[] getKvData(String keyVal) {
        String url = keyVal.split("\\.")[1];
        return url.split("@");
    }

    private String convertListToString(List<String> list) {
        StringBuilder mnemonicStr = new StringBuilder();
        for (String mnemonic : list) {
            mnemonicStr.append(mnemonic + ",");
        }
        return mnemonicStr.toString();
    }
}