package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.BoundaryValue;
import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.RoutingItemInfo;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPFindParentBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPParallelSearchBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactResultBody;

import java.io.ObjectOutputStream;

/**
 * Created by shmin at 4/11/2019 10:16 PM
 **/
public class SPFindParentListener extends ActionAdapter {

    public SPFindParentListener(AbstractInstance instance)
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
            SPParallelSearchBody body = (SPParallelSearchBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null)
            {
                System.out.println("SP_FIND_PARENT: Tree node is null, do not process the message");
                return;
            }

            JcsTuple searchedData = body.getTuple();
            BoundaryValue minValue = treeNode.getContent().getMinValue();
            BoundaryValue maxValue = treeNode.getContent().getMaxValue();

            if (treeNode.getContent().satisfyRange(searchedData)) {
                // 找到了最合适的节点

            } else {
                // 没有找到，继续向上，当然最后肯定会找到的
                body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                thead.setMsgType(MsgType.SP_FIND_PARENT.getValue());
                result = new Message(thead, body);
                serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
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
        if (msg.getHead().getMsgType() == MsgType.SP_FIND_PARENT.getValue()) {
            // TODO: JUST FOR TEST
            System.out.println("***SP_FIND_PARENT");
            return true;
        }
        return false;
    }
}
