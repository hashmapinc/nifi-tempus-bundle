package com.hashmapinc.tempus.nifi.processors;

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

        HashSet<String> opcNodeList;

        // Extract file path from property field
        File filePath = new File(context.getProperty(STORAGE_FILE_NAME).getValue());
        opcNodeList = readStorageFile(filePath);

        // Initialize  response variable
        final AtomicReference<String> opcJsonData = new AtomicReference<>();

        // Read opc data from flow file content
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {

                try{
                    String tagname = new BufferedReader(new InputStreamReader(in)).readLine();
                    opcJsonData.set(tagname);
                }catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    logger.error("Failed to read");
                }
            }
        });

        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(opcJsonData.get());
        } catch (IOException ex) {
            logger.error("Error in parsing JSON string");
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> attributes = jsonNode.fields();
        while (attributes.hasNext()) {
            Map.Entry<String, JsonNode> attribute = attributes.next();
            processAttribute(attribute, opcNodeList);
        }

        if (!opcNodeList.isEmpty()) {
            final String outData = parseSetData(opcNodeList);
            session.putAttribute(flowFile, "filename", filePath.getName());
            flowFile = session.write(flowFile, out -> out.write(outData.getBytes()));
            session.transfer(flowFile, SUCCESS);
        } else {
            logger.error("OPCUA Set is empty");
            session.transfer(flowFile, FAILURE);
        }
    }

    private HashSet<String> readStorageFile(File filePath) {
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

    private void processAttribute(Map.Entry<String, JsonNode> attribute, HashSet<String> opcNodeList) {
        if (attribute.getKey().equals("deleted")) {
            removeDeletedAttributes(attribute.getValue(), opcNodeList);
        } else if (!attribute.getValue().asBoolean()) {
            opcNodeList.remove(attribute.getKey());
        } else {
            opcNodeList.add(attribute.getKey());
        }
    }

    private void removeDeletedAttributes(JsonNode attrList, HashSet<String> opcNodeList) {
        Iterator<JsonNode> iterator = attrList.elements();
        while (iterator.hasNext()) {
            JsonNode js = iterator.next();
            opcNodeList.remove(js.textValue());
        }
    }

    private String parseSetData(HashSet<String> opcNodeList) {
        StringBuilder nodeListData = new StringBuilder();
        Iterator<String> it = opcNodeList.iterator();
        while (it.hasNext()) {
            nodeListData.append(it.next() + System.getProperty("line.separator"));
        }
        return nodeListData.toString();
    }
}
