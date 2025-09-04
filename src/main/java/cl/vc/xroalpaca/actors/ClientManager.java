package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.routing.RoundRobinPool;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ClientManager extends AbstractActor {

    @Getter
    private static final BiMap<String, ActorRef> orderHasmapClient = HashBiMap.create();

    public static Props props() {
        return Props.create(ClientManager.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onMessages)
                .match(NotificationMessage.Notification.class, this::onNotificationConnect)
                .build();
    }

    private void onMessages(TransportingObjects conn) {
        try {

            if (conn.getMessage() instanceof SessionsMessage.Connect) {
                onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Disconnect) {
                onDisconnect(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.NewOrderRequest) {
                onNewOrderRequest(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderReplaceRequest) {
                onOrderReplaceRequest(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderCancelRequest) {
                onCancelOrderRequest(conn);
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onNotificationConnect(NotificationMessage.Notification msg) {
        try {

            orderHasmapClient.forEach((key, value) -> value.tell(msg, ActorRef.noSender()));

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    public void onConnect(TransportingObjects conn) {
        try {

            String idconnection = conn.getCtx().channel().id().toString();

            if (!orderHasmapClient.containsKey(idconnection)) {
                ActorRef clientManagerPool = getContext().actorOf(new RoundRobinPool(1)
                        .props(OrderManager.props(conn.getCtx().channel())), idconnection);
                orderHasmapClient.put(idconnection, clientManagerPool);
            } else {
                orderHasmapClient.get(idconnection).tell(conn.getCtx().channel(), ActorRef.noSender());
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onDisconnect(TransportingObjects conn) {
        try {
            if (orderHasmapClient.containsKey(conn.getCtx().channel().id().toString())) {
                ActorRef client = orderHasmapClient.get(conn.getCtx().channel().id().toString());
                client.tell(new OrderManager.Disconnect(), ActorRef.noSender());
                orderHasmapClient.remove(conn.getCtx().channel().id().toString());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onNewOrderRequest(TransportingObjects conn) {
        try {
            String idconnection = conn.getCtx().channel().id().toString();
            orderHasmapClient.get(idconnection).tell(conn.getMessage(), ActorRef.noSender());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onOrderReplaceRequest(TransportingObjects conn) {
        try {
            String idconnection = conn.getCtx().channel().id().toString();
            if (orderHasmapClient.containsKey(idconnection)) {
                orderHasmapClient.get(idconnection).tell(conn.getMessage(), ActorRef.noSender());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onCancelOrderRequest(TransportingObjects conn) {
        try {
            String idconnection = conn.getCtx().channel().id().toString();
            if (orderHasmapClient.containsKey(idconnection)) {
                orderHasmapClient.get(idconnection).tell(conn.getMessage(), getSelf());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


}
