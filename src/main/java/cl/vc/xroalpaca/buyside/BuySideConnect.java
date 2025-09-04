package cl.vc.xroalpaca.buyside;

import akka.actor.AbstractActor;
import akka.actor.Props;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import quickfix.SessionID;

import java.util.HashMap;


@Slf4j
public class BuySideConnect extends AbstractActor {

    @Getter
    private static ChannelHandlerContext clientSesionSellside;

    @Getter
    private static HashMap<String, SessionID> clientBuyside = new HashMap<>();

    public static Props props() {
        return Props.create(BuySideConnect.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onMessages)
                .match(NewOrder.class, this::onNewOrderRequest)
                .match(ReplaceOrder.class, this::onOrderReplaceRequest)
                .match(CancelOrder.class, this::onCancelOrderRequest)
                .build();
    }

    private void onMessages(TransportingObjects conn) {

        try {

            if (conn.getMessage() instanceof RoutingMessage.Order) {
                //onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Connect) {
                onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Disconnect) {
                onDisconnect(conn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void onConnect(TransportingObjects conn) {
        try {

            clientSesionSellside = conn.getCtx();
            log.info("BuySideConnect");

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onDisconnect(TransportingObjects conn) {
        try {

            clientSesionSellside = null;
            log.error("se desconecta el usuario/componente buyside, no hay a quiern entregarle la orden ");

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onNewOrderRequest(NewOrder conn) {
        try {

            if (clientBuyside.containsKey(conn.getMessage().getOrder().getClOrdId())) {
                clientBuyside.put(conn.getMessage().getOrder().getClOrdId(), conn.getSessionID());
            }

            if (clientSesionSellside != null) {
                clientSesionSellside.channel().writeAndFlush(conn.getMessage());
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onOrderReplaceRequest(ReplaceOrder conn) {
        try {


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onCancelOrderRequest(CancelOrder conn) {
        try {


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    @Value
    public static class NewOrder {
        @NonNull
        RoutingMessage.NewOrderRequest message;
        @NonNull
        SessionID sessionID;

    }

    @Value
    public static class ReplaceOrder {
        @NonNull
        RoutingMessage.OrderReplaceRequest message;
        @NonNull
        SessionID sessionID;

    }

    @Value
    public static class CancelOrder {
        @NonNull
        RoutingMessage.OrderReplaceRequest message;
        @NonNull
        SessionID sessionID;

    }

}
