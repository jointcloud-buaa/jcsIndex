package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPDeleteParentBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateTagBody;

import java.io.ObjectOutputStream;

/**
 * Created by shmin at 4/24/2019 1:28 AM
 **/
public class SPDeleteParentListener extends ActionAdapter{
    public SPDeleteParentListener(AbstractInstance instance)
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
            SPDeleteParentBody body = (SPDeleteParentBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null) {
                System.out.println("SP_DELETE_PARENT: Tree node is null, do not process the message");
                return;
            }

            JcsTuple deletedData = body.getTuple();

            if (treeNode.getContent().satisfyRange(deletedData)) {
                // 符合要求, 数据应该在这个节点，删除它
                treeNode.getContent().deleteJcs(deletedData);
            } else {
                // 继续向上传递
                body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                head.setMsgType(MsgType.SP_DELETE_PARENT.getValue());
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
        if (msg.getHead().getMsgType() == MsgType.SP_DELETE_PARENT.getValue()) {
            // TODO: JUST FOR TEST
            System.out.println("***SP_DELETE_PARENT");
            return true;
        }
        return false;
    }
}

