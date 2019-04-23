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

public class SPDeleteJcsListener extends ActionAdapter
{

    public SPDeleteJcsListener(AbstractInstance instance)
    {
        super(instance);
    }

    public void actionPerformed(ObjectOutputStream oos, Message msg) throws EventHandleException
    {
        super.actionPerformed(oos, msg);

        Message result = null;
        Head head = new Head();

        try
        {
            /* get the handler of the ServerPeer */
            ServerPeer serverpeer = (ServerPeer) instance.peer();

            /* get the message body */
            SPDeleteJcsBody body = (SPDeleteJcsBody) msg.getBody();

            TreeNode treeNode = serverpeer.getTreeNode(body.getLogicalDestination());
            if (treeNode == null)
            {
                System.out.println("SP_PUBLISH: Tree node is null, do not process the message");
                return;
            }

            JcsTuple insertedData = body.getTuple();

            BoundaryValue minValue = treeNode.getContent().getMinValue();
            BoundaryValue maxValue = treeNode.getContent().getMaxValue();

            if ((insertedData.compareTo(minValue) >= 0) && (insertedData.compareTo(maxValue) < 0))
            {
                // 只有符合要求才在该处插入，否则传递给父节点
                if (treeNode.getContent().satisfyRange(insertedData)) {
                    // 数据在该节点，可以删除
                    treeNode.getContent().deleteJcs(insertedData);
                } else {
                    // 数据可能在Parent上，所以需要向上传递
                    // 向父节点发送PUBLISH消息
                    body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                    head.setMsgType(MsgType.SP_DELETE_PARENT.getValue());
                    result = new Message(head, new SPDeleteParentBody(body));
                    serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
                }
            }
            else
            {
                body.setPhysicalSender(serverpeer.getPhysicalInfo());
                body.setLogicalSender(treeNode.getLogicalInfo());

                if (minValue.compareTo(insertedData) > 0)
                {
                    int index = treeNode.getLeftRoutingTable().getTableSize() - 1;
                    int found = -1;
                    while ((index >= 0) && (found == -1))
                    {
                        RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
                        if (nodeInfo != null)
                        {
                            if (nodeInfo.getMaxValue().compareTo(insertedData)>0)
                            {
                                found = index;
                            }
                        }
                        index--;
                    }

                    if (found != -1)
                    {
                        RoutingItemInfo transferInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(found);
                        body.setLogicalDestination(transferInfo.getLogicalInfo());

                        head.setMsgType(MsgType.SP_DELETE_JCS.getValue());
                        result = new Message(head, body);
                        serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
                    }
                    else
                    {
                        if (treeNode.getLeftChild() != null)
                        {
                            body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

                            head.setMsgType(MsgType.SP_DELETE_JCS.getValue());
                            result = new Message(head, body);
                            serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
                        }
                        else
                        {
                            if (treeNode.getLeftAdjacentNode() != null)
                            {
                                body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

                                head.setMsgType(MsgType.SP_DELETE_JCS.getValue());
                                result = new Message(head, body);
                                serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
                            }
                            else
                            {
                                System.out.println("最左的节点，实际上这句话不应该输出");
                                // treeNode.getContent().insertData(insertedData, 1);
                                // SPGeneralAction.updateRangeValues(serverpeer, treeNode);
                            }
                        }
                    }
                }
                else
                {
                    int index = treeNode.getRightRoutingTable().getTableSize() - 1;
                    int found = -1;
                    while ((index >= 0) && (found == -1))
                    {
                        RoutingItemInfo nodeInfo = (RoutingItemInfo)treeNode.getRightRoutingTable().getRoutingTableNode(index);
                        if (nodeInfo != null)
                        {
                            if (nodeInfo.getMinValue().compareTo(insertedData)<=0)
                            {
                                found = index;
                            }
                        }
                        index--;
                    }

                    if (found != -1)
                    {
                        RoutingItemInfo transferInfo = treeNode.getRightRoutingTable().getRoutingTableNode(found);
                        body.setLogicalDestination(transferInfo.getLogicalInfo());

                        head.setMsgType(MsgType.SP_DELETE_JCS.getValue());
                        result = new Message(head, body);
                        serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
                    }
                    else
                    {
                        if (treeNode.getRightChild() != null)
                        {
                            body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

                            head.setMsgType(MsgType.SP_DELETE_JCS.getValue());
                            result = new Message(head, body);
                            serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
                        }
                        else
                        {
                            if (treeNode.getRightAdjacentNode() != null)
                            {
                                body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

                                head.setMsgType(MsgType.SP_DELETE_JCS.getValue());
                                result = new Message(head, body);
                                serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
                            }
                            else
                            {
                                System.out.println("最右的节点，实际上这句话不应该输出");
                                // treeNode.getContent().insertData(insertedData, 2);
                                // SPGeneralAction.updateRangeValues(serverpeer, treeNode);
                            }
                        }
                    }
                }
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
        if (msg.getHead().getMsgType() == MsgType.SP_DELETE_JCS.getValue()) {
            // TODO: JUST FOR TEST
            System.out.println("***SP_DELETE_JCS");
            return true;
        }
        return false;
    }

}
