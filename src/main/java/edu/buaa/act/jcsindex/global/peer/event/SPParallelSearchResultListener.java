package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.IndexValue;
import edu.buaa.act.jcsindex.global.peer.info.TreeNode;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.SPParallelSearchResultBody;
import edu.buaa.act.jcsindex.global.protocol.body.SPSearchExactResultBody;

import java.io.ObjectOutputStream;

/**
 * Created by shmin at 4/20/2019 1:43 AM
 **/
public class SPParallelSearchResultListener extends ActionAdapter
{

    public SPParallelSearchResultListener(AbstractInstance instance)
    {
        super(instance);
    }

    public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
    {
        super.actionPerformed(oos, msg);

        Message result = null;
        Head thead = new Head();

        try
        {
            /* get the handler of the ServerPeer */
            ServerPeer serverpeer = (ServerPeer) instance.peer();

            /* get the message body */
            SPParallelSearchResultBody body = (SPParallelSearchResultBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null)
            {
                System.out.println("SP_SEARCH_EXACT_RESULT: Tree node is null, do not process the message");
                return;
            }
            // TODO: JUST FOR TEST
            if (body.getDests().size()  != 0) {
                // 确实获得了结果
                for (String dest : body.getDests()) {
                    System.out.print(dest + "\t");
                }
                System.out.println();
            } else {
                // 没有获得结果
                System.out.println("No Result");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new EventHandleException("Super peer returns exact search result failure", e);
        }
    }

    public boolean isConsumed(Message msg) throws EventHandleException
    {
        if (msg.getHead().getMsgType() == MsgType.SP_PARALLEL_SEARCH_RESULT.getValue()) {
            // TODO: JUST FOR TEST
            System.out.println("***SP_PARALLEL_SEARCH_RESULT");
            return true;
        }
        return false;
    }
}
