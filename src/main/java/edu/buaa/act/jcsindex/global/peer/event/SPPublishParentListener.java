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
import edu.buaa.act.jcsindex.global.protocol.body.*;

import java.io.ObjectOutputStream;

/**
 * Created by shimin at 4/11/2019 4:01 PM
 **/
public class SPPublishParentListener extends ActionAdapter{
    public SPPublishParentListener(AbstractInstance instance)
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
            SPPublishParentBody body = (SPPublishParentBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null) {
                System.out.println("SP_PUBLISH_PARENT: Tree node is null, do not process the message");
                return;
            }

            JcsTuple insertedData = body.getTuple();

            if (treeNode.getContent().satisfyRange(insertedData)) {
                // 符合要求才在此处插入，否则向上传递
                treeNode.getContent().insertJcsTuple(insertedData);
                // 更新右孩子的tagValue
                if (treeNode.getRightChild() != null) {
                    body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());
                    head.setMsgType(MsgType.SP_UPDATE_TAG.getValue());
                    result = new Message(head, new SPUpdateTagBody(body));
                    serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
                }
            } else {
                // 更新本节点的tagValue
                treeNode.getContent().setTagLeftSets(insertedData);
                // 向父节点发送PUBLISH_PARENT消息
                body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                head.setMsgType(MsgType.SP_PUBLISH_PARENT.getValue());
                result = new Message(head, body);
                serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
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
        if (msg.getHead().getMsgType() == MsgType.SP_PUBLISH_PARENT.getValue()) {
            // TODO: JUST FOR TEST
            return true;
        }
        return false;
    }
}
