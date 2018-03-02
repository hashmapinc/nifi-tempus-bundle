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
import org.apache.nifi.util.StringUtils;

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

@Tags({"Tempus", "Json", "Trajectory"})
@CapabilityDescription("Read a Json file and convert to Tempus Device Telemetry Json format.")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class TrajectoryToTempus extends AbstractProcessor {

    private static ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor DEVICE_NAME = new PropertyDescriptor
            .Builder().name("Device Name")
            .displayName("Device Name")
            .description("This property will direct the processor to output the Tempus Device name.")
            .required(true)
            .defaultValue("nameWell")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRAJECTORY_TYPE = new PropertyDescriptor
            .Builder().name("Trajectory Type")
            .displayName("Trajectory Type")
            .description("This property will direct the processor to output the Tempus Trajectory data.")
            .required(true)
            .defaultValue("ds")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCEFIELD_NAME = new PropertyDescriptor
            .Builder().name("Source Field Name")
            .displayName("Source Field Name")
            .description("This property will direct the processor to output the Tempus Trajectory data. eg:mdValue ")
            .required(true)
            .defaultValue("mdValue")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_CONSTRUCT = new PropertyDescriptor
            .Builder().name("Key Construct")
            .displayName("Key Construct")
            .description("This property will direct the processor to output the key value for Tempus UI. For Trajectory it is Trajectory name" +
                    "  eg:name")
            .required(true)
            .defaultValue("name")
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
        descriptors.add(TRAJECTORY_TYPE);
        descriptors.add(SOURCEFIELD_NAME);
        descriptors.add(KEY_CONSTRUCT);
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
        String trajectoryType = context.getProperty(TRAJECTORY_TYPE).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String sourceFieldName = context.getProperty(SOURCEFIELD_NAME).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");
        String keyConstruct = context.getProperty(KEY_CONSTRUCT).evaluateAttributeExpressions(flowFile).getValue().replaceAll("[;\\s\t]", "");

        String deviceName = flowFile.getAttribute(deviceNameProperty);
        JsonNode jsonRequest = null;
        String tempusKeyValue = null;

        if(!StringUtils.isBlank(deviceName)){

            InputStream inputStream = session.read(flowFile);
            try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))){
                tempusKeyValue =  flowFile.getAttribute(sourceFieldName);//jsonRequest.get(sourceFieldName).textValue();//logObject.getIndex();
            }catch(IOException ex){
                log.error("Error reading flow file contents "+ex.getMessage());
                session.transfer(flowFile, FAILURE);
            }
            ObjectNode trajResponse = mapper.createObjectNode();
            String outData = null;
            if(NifiJsonUtil.TS_FIELDNAME.equalsIgnoreCase(trajectoryType)){
                long longTime = NifiJsonUtil.changeTimeToLong(tempusKeyValue);
                trajResponse.put(trajectoryType,String.valueOf(longTime));
            }else{
                trajResponse.put(trajectoryType,tempusKeyValue);
           }

            StringTokenizer attributeTokenizer = new StringTokenizer(keyConstruct,NifiJsonUtil.KEY_CONSTRUCT_DELIMETER);

            StringBuffer valueFiledBuffer = new StringBuffer();

            while(attributeTokenizer.hasMoreTokens()){
                String attributeName = attributeTokenizer.nextToken();
                try{
                    String attributeVale = flowFile.getAttribute(attributeName);//jsonRequest.get(attributeName).textValue();
                    if(attributeVale == null || attributeVale.isEmpty()){
                        valueFiledBuffer.append(attributeName);
                    }else{
                        valueFiledBuffer.append(attributeVale);
                    }

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

          //  List<ObjectNode> trajStnList = new ArrayList<>();

            ObjectNode trajNode = mapper.createObjectNode();
            String aziRef = flowFile.getAttribute("aziRef");
            String aziValue = flowFile.getAttribute("aziValue");
            String aziVertSectValue = flowFile.getAttribute("aziVertSectValue");
            String aziVertSectUom = flowFile.getAttribute("aziVertSectUom");
            String cmnDataDtimCreation = flowFile.getAttribute("cmnDataDtimCreation");
            String cmnDataDtimLstChange = flowFile.getAttribute("cmnDataDtimLstChange");
            String dispEwValue = flowFile.getAttribute("dispEwValue");
            String dispEwUom = flowFile.getAttribute("dispEwUom");
            String dispEwVertSecOrigValue = flowFile.getAttribute("dispEwVertSecOrigValue");
            String dispEwVertSecOrigUom = flowFile.getAttribute("dispEwVertSecOrigUom");
            String dispNsValue = flowFile.getAttribute("dispNsValue");
            String dispNsUom = flowFile.getAttribute("dispNsUom");
            String dispNsVertSecOrigValue = flowFile.getAttribute("dispNsVertSecOrigValue");
            String dispNsVertSecOrigUom = flowFile.getAttribute("dispNsVertSecOrigUom");
            String dlsUom = flowFile.getAttribute("dlsUom");
            String dlsValue = flowFile.getAttribute("dlsValue");
            String dtimStn = flowFile.getAttribute("dtimStn");
            String dtimTrajStart = flowFile.getAttribute("dtimTrajStart");
            String inclValue = flowFile.getAttribute("inclValue");
            String inclUom = flowFile.getAttribute("inclUom");
            String mdMnValue = flowFile.getAttribute("mdMnValue");
            String mdMnUom = flowFile.getAttribute("mdMnUom");
            String mdMxValue = flowFile.getAttribute("mdMxValue");
            String mdMxUom = flowFile.getAttribute("mdMxUom");
            String mdValue = flowFile.getAttribute("mdValue");
            String mdUom = flowFile.getAttribute("mdUom");
            String typeTrajStation = flowFile.getAttribute("typeTrajStation");
            String uid = flowFile.getAttribute("uid");
            String vertSectValue = flowFile.getAttribute("vertSectValue");
            String vertSectUom = flowFile.getAttribute("vertSectUom");

            String nameTrajectory = flowFile.getAttribute("name");
            String tvdValue = flowFile.getAttribute("tvdValue");
            String aziUom = flowFile.getAttribute("aziUom");


            String nameWellbore = flowFile.getAttribute("nameWellbore");

            trajNode.put("nameWell",deviceName);


            if(!StringUtils.isBlank(nameWellbore)){
                trajNode.putPOJO("nameWellbore",nameWellbore);
            }
            if(!StringUtils.isBlank(nameTrajectory)){
                trajNode.putPOJO("nameTrajectory",nameTrajectory);
            }

            if(!StringUtils.isBlank(uid)){
                trajNode.putPOJO("nameTrajectoryStn",uid);
            }

            if(!StringUtils.isBlank(typeTrajStation)){
                trajNode.putPOJO("trajectoryStnType",typeTrajStation);
            }

            if(!StringUtils.isBlank(aziRef)){
                trajNode.putPOJO("aziRef",aziRef);
            }
            if(!StringUtils.isBlank(aziValue)){
                trajNode.put("aziValue",aziValue);
            }
            if(!StringUtils.isBlank(aziUom)){
                trajNode.put("aziUom",aziUom);
            }
            if(!StringUtils.isBlank(aziVertSectValue)){
                trajNode.put("aziVertSectValue",aziVertSectValue);
            }

            if(!StringUtils.isBlank(aziVertSectUom)){
                trajNode.put("aziVertSectUom",aziVertSectUom);
            }

            if(!StringUtils.isBlank(cmnDataDtimCreation)){
                trajNode.put("cmnDataDtimCreation",NifiJsonUtil.changeLongToDateTime(cmnDataDtimCreation));
            }
            if(!StringUtils.isBlank(cmnDataDtimLstChange)){
                trajNode.putPOJO("cmnDataDtimLstChange",NifiJsonUtil.changeLongToDateTime(cmnDataDtimLstChange));
            }
            if(!StringUtils.isBlank(dispEwValue)){
                trajNode.put("dispEwValue",dispEwValue);
            }

            if(!StringUtils.isBlank(dispEwUom)){
                trajNode.put("dispEwUom",dispEwUom);
            }


            if(!StringUtils.isBlank(dispEwVertSecOrigValue)){
                trajNode.put("dispEwVertSecOrigValue",dispEwVertSecOrigValue);
            }

            if(!StringUtils.isBlank(dispEwVertSecOrigUom)){
                trajNode.put("dispEwVertSecOrigUom",dispEwVertSecOrigUom);
            }

            if(!StringUtils.isBlank(dispNsValue)){
                trajNode.put("dispNsValue",dispNsValue);
            }

            if(!StringUtils.isBlank(dispNsUom)){
                trajNode.put("dispNsUom",dispNsUom);
            }

            if(!StringUtils.isBlank(dispNsVertSecOrigValue)){
                trajNode.putPOJO("dispNsVertSecOrigValue",dispNsVertSecOrigValue);
            }

            if(!StringUtils.isBlank(dispNsVertSecOrigUom)){
                trajNode.put("dispNsVertSecOrigUom",dispNsVertSecOrigUom);
            }
            if(!StringUtils.isBlank(dlsUom)){
                trajNode.put("dlsUom", dlsUom);
            }
            if(!StringUtils.isBlank(dlsValue)){
                trajNode.put("dlsValue",dlsValue);
            }
            if(!StringUtils.isBlank(dtimStn)){
                trajNode.put("dtimStn",dtimStn);
            }
            if(!StringUtils.isBlank(dtimTrajStart)){
                trajNode.put("dtimTrajStart",NifiJsonUtil.changeLongToDateTime(dtimTrajStart));
            }
            if(!StringUtils.isBlank(inclValue)){
                trajNode.put("inclValue",inclValue);
            }
            if(!StringUtils.isBlank(inclUom)){
                trajNode.put("inclUom",inclUom);
            }
            if(!StringUtils.isBlank(mdMnValue)){
                trajNode.put("mdMnValue",mdMnValue);
            }
            if(!StringUtils.isBlank(mdMnUom)){
                trajNode.put("mdMnUom",mdMnUom);
            }
            if(!StringUtils.isBlank(mdMxValue)){
                trajNode.put("mdMxValue",mdMxValue);
            }

            if(!StringUtils.isBlank(mdMxUom)){
                trajNode.put("mdMxUom",mdMxUom);
            }

            if(!StringUtils.isBlank(mdValue)){
                trajNode.put("mdValue",mdValue);
            }

            if(!StringUtils.isBlank(mdUom)){
                trajNode.put("mdUom",mdUom);
            }

            if(!StringUtils.isBlank(tvdValue)){
                trajNode.put("tvdValue",tvdValue);
            }

            if(!StringUtils.isBlank(vertSectValue)){
                trajNode.put("vertSectValue",vertSectValue);
            }

            if(!StringUtils.isBlank(vertSectUom)){
                trajNode.put("vertSectUom",vertSectUom);
            }
            trajNode.put("tempus.hint","TRAJECTORY");

            //trajStnList.add(trajNode);
            valueNode.putPOJO(valueFiledBuffer.toString(), trajNode);
            trajResponse.putPOJO(NifiJsonUtil.LOG_CHILD_ELEMENT,valueNode);

            List<ObjectNode> trajObjectList = new ArrayList<>();
            trajObjectList.add(trajResponse);
            ArrayNode array = mapper.valueToTree(trajObjectList);
            ObjectNode logParentNode = mapper.createObjectNode();

            logParentNode.putArray(deviceName).addAll(array);

           // logParentNode.putPOJO(deviceName,trajResponse);

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
