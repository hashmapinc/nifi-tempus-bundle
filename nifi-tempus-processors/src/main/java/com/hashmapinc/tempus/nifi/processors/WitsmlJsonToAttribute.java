package com.hashmapinc.tempus.nifi.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"Tempus", "Witsml", "WitsmlObjects", "Attributes"})
@CapabilityDescription("Read the witsml storage file for Well, Wellbore, Object Ids. Convert the Json data to flowfile attributes which will be input to GetData Processor")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class WitsmlJsonToAttribute extends AbstractProcessor {
    private static ObjectMapper mapper = new ObjectMapper();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Splits")
            .description("Read the witsml storage file and write the uids to flowfile attributes.")
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("Original")
            .description("Transfer the original data to flowfile")
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
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(ORIGINAL);

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
        FlowFile original = session.get();

        if (original == null ) {
            logger.error("Flow file is null");
            return;
        }

        // Initialize  response variable
        final AtomicReference<String> flowfileData = new AtomicReference<>();

        // Read opc data from flow file content
        session.read(original, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {

                try{
                    String data = new BufferedReader(new InputStreamReader(in)).readLine();
                    flowfileData.set(data);
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
            session.transfer(original, FAILURE);
            return;
        }

        Iterator<Map.Entry<String, JsonNode>> it = jsonNode.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> attribute = it.next();
            String[] keyVal = attribute.getKey().split("\\.");
            String[] uids = keyVal[1].split("/");

            FlowFile split = session.create(original);
            session.putAttribute(split, "nameWell", keyVal[0]);
            session.putAttribute(split, "uidWell", uids[0]);
            session.putAttribute(split, "uidWellbore", uids[1]);
            session.putAttribute(split, "id", uids[2]);
            session.putAttribute(split, "mnemonics", attribute.getValue().asText());
            final String outData = attribute.toString();
            split = session.write(split, out -> out.write(outData.getBytes()));
            session.transfer(split, SUCCESS);
        }

        session.transfer(original, ORIGINAL);
    }
}