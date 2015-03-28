package org.wso2.siddhi.debs2015.extensions.window;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.parser.CollectionOperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

public class CustomExternalTimeWindowProcessor extends WindowProcessor implements FindableProcessor {
    static final Logger log = Logger.getLogger(CustomExternalTimeWindowProcessor.class);
    private long timeToKeep;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;
    private VariableExpressionExecutor timeStampVariableExpressionExecutor;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.expiredEventChunk = new ComplexEventChunk<StreamEvent>();
        if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
            timeToKeep = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
        } else {
            timeToKeep = Long.parseLong(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
        }
        timeStampVariableExpressionExecutor = ((VariableExpressionExecutor) attributeExpressionExecutors[0]);
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        while (streamEventChunk.hasNext()) {

            StreamEvent streamEvent = streamEventChunk.next();
            long currentTime = (Long) streamEvent.getAttribute(timeStampVariableExpressionExecutor.getPosition());

            StreamEvent clonedEvent = null;
            if (streamEvent.getType() == StreamEvent.Type.EXPIRED) {
                clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);
                clonedEvent.setTimestamp(currentTime + timeToKeep);
            }

            while (expiredEventChunk.hasNext()) {
                StreamEvent expiredEvent = expiredEventChunk.next();
                long timeDiff = expiredEvent.getTimestamp() - currentTime;
                if (timeDiff <= 0) {
                    expiredEventChunk.remove();
                    streamEventChunk.insertBeforeCurrent(expiredEvent);
                } else {
                    expiredEventChunk.reset();
                    break;
                }
            }

            if (streamEvent.getType() == StreamEvent.Type.EXPIRED) {
                this.expiredEventChunk.add(clonedEvent);
                streamEventChunk.remove();
            }
            expiredEventChunk.reset();
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{expiredEventChunk};
    }

    @Override
    public void restoreState(Object[] state) {
        expiredEventChunk = (ComplexEventChunk<StreamEvent>) state[0];
    }

    @Override
    public StreamEvent find(ComplexEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, expiredEventChunk,streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MetaComplexEvent metaComplexEvent, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap, int matchingStreamIndex, long withinTime) {
        return CollectionOperatorParser.parse(expression, metaComplexEvent, executionPlanContext, variableExpressionExecutors, eventTableMap, matchingStreamIndex, inputDefinition, withinTime);
    }
}