package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import cl.vc.xroalpaca.MainApp;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.Message;
import io.netty.channel.Channel;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class OrderManager extends AbstractActor {

    private static final int ACTORS_BUFFER_SIZE = 10000;
    private final HashMap<String, ActorRef> orderTrackers = new HashMap<>();
    private Channel channel;
    private ConcurrentLinkedQueue<ActorRef> availableActors = new ConcurrentLinkedQueue<>();
    private RoutingMessage.OrderCancelReject.Builder reject = RoutingMessage.OrderCancelReject.newBuilder();


    private OrderManager(Channel channel) {
        this.channel = channel;
        reject.setText("Repeated ID");

        while (availableActors.size() < ACTORS_BUFFER_SIZE) {
            if (MainApp.simulador) {
                ActorRef newActor = getContext().actorOf(OrderTrackerSimulador.props(channel));
                availableActors.add(newActor);
            } else {
                ActorRef newActor = getContext().actorOf(OrderTracker.props(channel));
                availableActors.add(newActor);
            }
        }
    }

    public static Props props(Channel channel) {
        return Props.create(OrderManager.class, channel);
    }

    @Override
    public void preStart() {

    }

    @Override
    public void postStop() {
        log.info("Tracker manager for {} terminated.", this.channel);
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Channel.class, this::onChannell)
                .match(Disconnect.class, this::onClientDisconnected)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrderRequest)
                .match(RoutingMessage.OrderReplaceRequest.class, this::onReplaceOrderRequest)
                .match(RoutingMessage.OrderCancelRequest.class, this::onCancelOrderRequest)
                .match(NotificationMessage.Notification.class, this::onNotificationConnect)
                .build();
    }

    private void onChannell(Channel msg) {
        try {

            this.channel = msg;
            orderTrackers.forEach((key, value) -> value.tell(msg, ActorRef.noSender()));
            availableActors.forEach(s -> {
                s.tell(msg, ActorRef.noSender());
            });

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    private void onNotificationConnect(NotificationMessage.Notification msg) {
        try {

            channel.writeAndFlush(msg);

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    private void sendMessage(Message message) {
        channel.writeAndFlush(message);
    }

    private void notifyError(Exception e) {
        NotificationMessage.Notification notification = NotificationUtil.buildErrorNotification(MainApp.getSecurityExchange(),
                        e.getMessage(), "OrderTrackerManager");
        sendMessage(notification);
    }

    private void onClientDisconnected(Disconnect conn) {
        try {

            if (!MainApp.ordercancel) {
                return;
            }

            orderTrackers.forEach((orderID, tracker) -> {
                RoutingMessage.OrderCancelRequest cancel = RoutingMessage.OrderCancelRequest.newBuilder()
                        .setId(orderID).build();
                tracker.tell(cancel, getSelf());
            });

            getSelf().tell(PoisonPill.getInstance(), getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onNewOrderRequest(RoutingMessage.NewOrderRequest newOrderRequest) {

        try {

            if (orderTrackers.containsKey(newOrderRequest.getOrder().getId())) {
                reject.setId(newOrderRequest.getOrder().getId());
                channel.writeAndFlush(reject.build());
                return;
            }

            ActorRef tracker = assignOrCreateTracker();
            tracker.tell(newOrderRequest, getSelf());

            orderTrackers.put(newOrderRequest.getOrder().getId(), tracker);

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    private ActorRef assignOrCreateTracker() {

        ActorRef tracker;

        if (this.availableActors.isEmpty()) {
            tracker = getContext().actorOf(OrderTracker.props(channel));
            log.warn("No available OrderTracker, starting a new one...");
        } else {
            tracker = availableActors.remove();
        }

        return tracker;
    }

    private void onReplaceOrderRequest(RoutingMessage.OrderReplaceRequest replaceOrderRequest) {
        try {

            if (orderTrackers.containsKey(replaceOrderRequest.getId())) {
                orderTrackers.get(replaceOrderRequest.getId()).tell(replaceOrderRequest, getSelf());
            } else {
                MainApp.getMessageEventBus().publish(new Envelope(replaceOrderRequest.getId(), replaceOrderRequest));
            }

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    private void onCancelOrderRequest(RoutingMessage.OrderCancelRequest cancelOrderRequest) {
        try {

            if (orderTrackers.containsKey(cancelOrderRequest.getId())) {
                orderTrackers.get(cancelOrderRequest.getId()).tell(cancelOrderRequest, getSelf());
            } else {
                MainApp.getMessageEventBus().publish(new Envelope(cancelOrderRequest.getId(), cancelOrderRequest));
            }

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    @Value
    public static class Disconnect {
    }

}
