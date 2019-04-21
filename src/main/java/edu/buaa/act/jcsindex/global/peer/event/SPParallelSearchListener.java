package edu.buaa.act.jcsindex.global.peer.event;

import edu.buaa.act.jcsindex.global.AbstractInstance;
import edu.buaa.act.jcsindex.global.peer.ServerPeer;
import edu.buaa.act.jcsindex.global.peer.info.*;
import edu.buaa.act.jcsindex.global.peer.management.EventHandleException;
import edu.buaa.act.jcsindex.global.protocol.Head;
import edu.buaa.act.jcsindex.global.protocol.Message;
import edu.buaa.act.jcsindex.global.protocol.MsgType;
import edu.buaa.act.jcsindex.global.protocol.body.*;

import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Created by shimin at 4/11/2019 9:03 PM
 **/
public class SPParallelSearchListener extends ActionAdapter {

    public SPParallelSearchListener(AbstractInstance instance)
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
                System.out.println("SP_PARALLEL_SEARCH: Tree node is null, do not process the message");
                return;
            }

            JcsTuple searchedData = body.getTuple();
            BoundaryValue minValue = treeNode.getContent().getMinValue();
            BoundaryValue maxValue = treeNode.getContent().getMaxValue();

            if ((searchedData.compareTo(minValue) >= 0) && (searchedData.compareTo(maxValue) < 0))
            {
                treeNode.incNumOfQuery(1);

                // 算法步骤
                // 1. 首先需要定位到恰当的节点，这是SPParallelSearchListener的任务之一
                // 2. 向上递推到第一个subtree range包含查询区间的节点
                // 3. 广播查询到所有的该节点的所有子树(已经想到查询方案)
                // 4. 将结果携带好，传递给父节点继续查询
                // 5. 最后一个节点负责把结果返回给Requestor
                if (treeNode.getContent().satisfyRange(searchedData)) {
                    // 直接符合要求找到合适的节点
                    // 首先获取所有子树节点
//                    BroadcastClient client = new BroadcastClient(serverpeer.getPhysicalInfo().getIP());
                    // TODO: 这里需要注意，后续需要修改Baton网络的值
//                    List<String> ans = client.broadcastSearch(searchedData.getTimeIndex(), searchedData.getLeftBound(), searchedData.getRightBound());
                    List<String> ans = new ArrayList<>();
                    // 结合内容，返回给上层节点，需要实现一个新的消息类型
                    if (treeNode.getParentNode() != null) {
                        body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                        thead.setMsgType(MsgType.SP_SEARCH_PARENT.getValue());
                        SPSearchParentBody tbody = new SPSearchParentBody(body);
                        tbody.setDests(ans);
                        result = new Message(thead, tbody);
                        serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
                    } else {
                        // 从这里直接返回结果
                        thead.setMsgType(MsgType.SP_PARALLEL_SEARCH_RESULT.getValue());
                        SPParallelSearchResultBody tbody = new SPParallelSearchResultBody(body.getPhysicalSender(), body.getLogicalSender(), ans, null);
                        result = new Message(thead, tbody);
                        serverpeer.sendMessage(body.getPhysicalRequester(), result);
                    }
                } else {
                    // 把请求传递给父节点，理论上是需要一个新的逻辑(虽然会有很多重复的逻辑在里面)
                    // TODO: 是否有必要修改physicalSender和logicalSender
                    body.setLogicalDestination(treeNode.getParentNode().getLogicalInfo());
                    thead.setMsgType(MsgType.SP_FIND_PARENT.getValue());
                    result = new Message(thead, new SPFindParentBody(body));
                    serverpeer.sendMessage(treeNode.getParentNode().getPhysicalInfo(), result);
                }
            }
            else
            {
                body.setPhysicalSender(serverpeer.getPhysicalInfo());
                body.setLogicalSender(treeNode.getLogicalInfo());

                if (minValue.compareTo(searchedData) > 0)
                {
                    int index = treeNode.getLeftRoutingTable().getTableSize() - 1;
                    int found = -1;
                    while ((index >= 0) && (found == -1))
                    {
                        if (treeNode.getLeftRoutingTable().getRoutingTableNode(index) != null)
                        {
                            RoutingItemInfo nodeInfo = treeNode.getLeftRoutingTable().getRoutingTableNode(index);
                            if (nodeInfo.getMaxValue().compareTo(searchedData) > 0)
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

                        thead.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());
                        result = new Message(thead, body);
                        serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
                    }
                    else
                    {
                        if (treeNode.getLeftChild() != null)
                        {
                            body.setLogicalDestination(treeNode.getLeftChild().getLogicalInfo());

                            thead.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());
                            result = new Message(thead, body);
                            serverpeer.sendMessage(treeNode.getLeftChild().getPhysicalInfo(), result);
                        }
                        else
                        {
                            if (treeNode.getLeftAdjacentNode() != null)
                            {
                                body.setLogicalDestination(treeNode.getLeftAdjacentNode().getLogicalInfo());

                                thead.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());
                                result = new Message(thead, body);
                                serverpeer.sendMessage(treeNode.getLeftAdjacentNode().getPhysicalInfo(), result);
                            }
                            else
                            {
                                // 不应该发生这种情况
                                System.out.println("最左节点已找遍，没有找到");
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
                        if (treeNode.getRightRoutingTable().getRoutingTableNode(index) != null)
                        {
                            RoutingItemInfo nodeInfo = treeNode.getRightRoutingTable().getRoutingTableNode(index);
                            if (nodeInfo.getMinValue().compareTo(searchedData) <= 0)
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

                        thead.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());
                        result = new Message(thead, body);
                        serverpeer.sendMessage(transferInfo.getPhysicalInfo(), result);
                    }
                    else
                    {
                        if (treeNode.getRightChild() != null)
                        {
                            body.setLogicalDestination(treeNode.getRightChild().getLogicalInfo());

                            thead.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());
                            result = new Message(thead, body);
                            serverpeer.sendMessage(treeNode.getRightChild().getPhysicalInfo(), result);
                        }
                        else
                        {
                            if (treeNode.getRightAdjacentNode() != null)
                            {
                                body.setLogicalDestination(treeNode.getRightAdjacentNode().getLogicalInfo());

                                thead.setMsgType(MsgType.SP_PARALLEL_SEARCH.getValue());
                                result = new Message(thead, body);
                                serverpeer.sendMessage(treeNode.getRightAdjacentNode().getPhysicalInfo(), result);
                            }
                            else
                            {
                                // 不应该发生这种情况
                                System.out.println("最右节点已找遍，没找到");
                            }
                        }
                    }
                }
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
        if (msg.getHead().getMsgType() == MsgType.SP_PARALLEL_SEARCH.getValue()) {
            // TODO: JUST FOR TEST
            System.out.println("***SP_PARALLEL_SEARCH");
            return true;
        }
        return false;
    }
}
