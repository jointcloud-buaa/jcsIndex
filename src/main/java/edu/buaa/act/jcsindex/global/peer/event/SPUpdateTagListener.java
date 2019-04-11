package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.JcsTuple;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPPublishParentBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPUpdateTagBody;

import java.io.ObjectOutputStream;

/**
 * Created by shmin at 4/11/2019 4:26 PM
 **/
public class SPUpdateTagListener extends ActionAdapter{
    public SPUpdateTagListener(AbstractInstance instance)
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
            SPUpdateTagBody body = (SPUpdateTagBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null) {
                System.out.println("SP_PUBLISH: Tree node is null, do not process the message");
                return;
            }

            JcsTuple insertedData = body.getTuple();
            // 设置该节点的tagValue
            treeNode.getContent().setTagRightSets(insertedData);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new EventHandleException("Super peer inserts index failure", e);
        }

    }

    public boolean isConsumed(Message msg) throws EventHandleException
    {
        if (msg.getHead().getMsgType() == MsgType.SP_UPDATE_TAG.getValue()) {
            // TODO: JUST FOR TEST
            System.out.println("***SP_UPDATE_TAG");
            return true;
        }
        return false;
    }
}
