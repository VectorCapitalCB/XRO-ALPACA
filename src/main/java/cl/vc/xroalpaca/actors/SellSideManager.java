package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.xroalpaca.adapters.AlpacaAdapter;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class SellSideManager extends AbstractActor {

    @Getter
    private final HashMap<String, ActorRef> trackers = new HashMap<>();
    private AlpacaAdapter adapterMaps;
    private ActorRef clientManager;

    private SellSideManager(ActorRef clientManager) {
        try {

            this.clientManager = clientManager;
            adapterMaps = new AlpacaAdapter(getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Props props(ActorRef clientManager) {
        return Props.create(SellSideManager.class, clientManager);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SSNewOrder.class, this::onNewOrderRequest)
                .match(SSReplace.class, this::onReplaceRequest)
                .match(SSCancel.class, this::onCancelOrderRequest)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(RoutingMessage.OrderCancelReject.class, this::onrejected)
                .match(NotificationMessage.Notification.class, this::onNotificationConnect)
                .build();
    }


    @Override
    public void preStart() {
    }

    @Override
    public void postStop() {
    }

    private void onOrder(RoutingMessage.Order msg) {
        try {

            if (!trackers.containsKey(msg.getId())) {
                ActorRef newActor = getContext().actorOf(OrderAlpacaTraker.props(null));
                newActor.tell(msg, ActorRef.noSender());
                trackers.put(msg.getId(), newActor);
            } else {
                trackers.get(msg.getId()).tell(msg, getSelf());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onrejected(RoutingMessage.OrderCancelReject reject) {
        trackers.get(reject.getId()).tell(reject, getSelf());
    }


    @Value
    public static class SSNewOrder {
        RoutingMessage.NewOrderRequest order;
        ActorRef tracker;
    }

    private void onNewOrderRequest(SSNewOrder newOrder) {
        try {

            adapterMaps.putNewOrderSingle(newOrder.getOrder().getOrder());
            trackers.put(newOrder.getOrder().getOrder().getId(), newOrder.getTracker());

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    @Value
    public static class SSReplace {
        RoutingMessage.OrderReplaceRequest replaceRequest;
        ActorRef tracker;
        RoutingMessage.Order order;
    }

    private void onReplaceRequest(SSReplace msg) {
        try {

            adapterMaps.putOrderReplace(msg.getReplaceRequest(), msg.getOrder());

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    @Value
    public static class SSCancel {
        RoutingMessage.OrderCancelRequest cancelRequest;
        ActorRef tracker;
        RoutingMessage.Order order;
    }

    private void onCancelOrderRequest(SSCancel msg) {
        try {

            adapterMaps.putOrderCancel(msg.getCancelRequest(), msg.getOrder());

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    private void onNotificationConnect(NotificationMessage.Notification msg) {
        try {
            clientManager.tell(msg, ActorRef.noSender());
        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }
}