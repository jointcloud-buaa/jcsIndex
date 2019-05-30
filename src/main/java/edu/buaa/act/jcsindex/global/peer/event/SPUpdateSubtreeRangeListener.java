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
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateSubtreeRangeBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateTagBody;

import java.io.ObjectOutputStream;

/**
 * Created by shmin at 4/19/2019 11:51 PM
 **/
public class SPUpdateSubtreeRangeListener extends ActionAdapter{
    public SPUpdateSubtreeRangeListener(AbstractInstance instance)
    {
        super(instance);
    }

    public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
    {
        super.actionPerformed(oos, msg);

        Message result = null;
        Head head = new Head();

        try {
            /* get the handler of the ServerPeer */
            ServerPeer serverpeer = (ServerPeer) instance.peer();

            /* get the message body */
            SPUpdateSubtreeRangeBody body = (SPUpdateSubtreeRangeBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null) {
                System.out.println("SP_PUBLISH: Tree node is null, do not process the message");
                return;
            }

            if (treeNode.getParentNode() != null) {
                BoundaryValue value = body.getValue();
                // 设置该节点的subtree范围
                treeNode.getContent().setSubtreeRangeL(value);

                serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), msg);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new EventHandleException("Super peer inserts index failure", e);
        }

    }

    public boolean isConsumed(Message msg) throws EventHandleException
    {
        if (msg.getHead().getMsgType() == MsgType.SP_UPDATE_SUBTREE_RANGE.getValue()) {
            // TODO: JUST FOR TEST
            return true;
        }
        return false;
    }
}
