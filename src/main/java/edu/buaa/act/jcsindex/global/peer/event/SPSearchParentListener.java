package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPParallelSearchBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPParallelSearchResultBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactResultBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchParentBody;

import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by shimin at 4/18/2019 10:22 PM
 **/
public class SPSearchParentListener extends ActionAdapter {

    public SPSearchParentListener(AbstractInstance instance)
    {
        super(instance);
    }

    public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
    {
        super.actionPerformed(oos, msg);

        Message result = null;
        Head thead = new Head();
        SPSearchExactResultBody resultBody = null;

        try
        {
            /* get the handler of the ServerPeer */
            ServerPeer serverpeer = (ServerPeer) instance.peer();

            /* get the message body */
            SPSearchParentBody body = (SPSearchParentBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null)
            {
                System.out.println("SP_FIND_PARENT: Tree node is null, do not process the message");
                return;
            }

            JcsTuple searchedData = body.getTuple();
            List<String> dests = body.getDests();
            BoundaryValue minValue = treeNode.getContent().getMinValue();
            BoundaryValue maxValue = treeNode.getContent().getMaxValue();

            // TODO: 没有使用tagSet的作用，直接返回，后续考虑加上这些功能

            Set<String> choosen = new HashSet<>(dests);
            choosen.addAll(treeNode.getContent().localSearch(searchedData.getTimeIndex(), searchedData.getLeftBound(), searchedData.getRightBound()));

            List<String> fdests = new ArrayList<>(choosen);
            if (treeNode.getParentNode() != null) {
                body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                thead.setMsgType(MsgType.SP_SEARCH_PARENT.getValue());
                body.setDests(fdests);
                result = new Message(thead, body);
                serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
            } else {
                // 从这里直接返回结果
                thead.setMsgType(MsgType.SP_PARALLEL_SEARCH_RESULT.getValue());
                SPParallelSearchResultBody tbody = new SPParallelSearchResultBody(body.getPhysicalSender(), body.getLogicalSender(), body.getRequestId(), fdests, null);
                result = new Message(thead, tbody);
                serverpeer.sendMessage(body.getPhysicalRequester(), result);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new EventHandleException("Super peer performs exact search failure", e);
        }
    }

    public boolean isConsumed(Message msg) throws EventHandleException
    {
        if (msg.getHead().getMsgType() == MsgType.SP_SEARCH_PARENT.getValue()) {
            // TODO: JUST FOR TEST
            return true;
        }
        return false;
    }
}
