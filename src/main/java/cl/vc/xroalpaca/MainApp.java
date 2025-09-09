package cl.vc.xroalpaca;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.RoundRobinPool;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufServer;
import cl.vc.xroalpaca.actors.ClientManager;
import cl.vc.xroalpaca.actors.SellSideManager;
import cl.vc.xroalpaca.actors.SendKafkaAndMongo;
import lombok.Data;
import lombok.Getter;

import java.io.FileInputStream;
import java.util.Properties;

@lombok.extern.slf4j.Slf4j
@Data
public class MainApp {

    @Getter
    public static Properties properties;

    @Getter
    public static ActorSystem system;

    public static NettyProtobufServer nettyProtobufServer;

    public static NettyProtobufServer nettyProtobufServerBuyside;

    @Getter
    private static ActorRef clientManager;

    @Getter
    private static ActorRef sellSideManager;

    @Getter
    private static ActorRef actorKafka;

    @Getter
    private static ActorRef buySideManager;

    @Getter
    private static RoutingMessage.SecurityExchangeRouting securityExchange;

    @Getter
    private static MessageEventBus messageEventBus = new MessageEventBus();

    public static void main(String[] args) {


        try (FileInputStream fis = new FileInputStream(args[0])) {

            properties = new Properties();
            properties.load(fis);

            if (properties != null) {

                log.info("Starting alpaca v1.3.1");

                securityExchange = RoutingMessage.SecurityExchangeRouting.valueOf(properties.getProperty("security-exchange"));

                system = ActorSystem.create("alpaca");

                clientManager = system.actorOf(new RoundRobinPool(1).props(ClientManager.props()));
                sellSideManager = system.actorOf(SellSideManager.props(clientManager));

                actorKafka = system.actorOf(new RoundRobinPool(1).props(SendKafkaAndMongo.props(properties)));

                nettyProtobufServer = new NettyProtobufServer(properties.getProperty("core.server.hostname"),
                        clientManager, properties.getProperty("path.logs"), securityExchange.name());
                new Thread(nettyProtobufServer).start();

            }

        } catch (Exception exc) {
            log.error("Error al leer parametros:", exc);
            System.exit(0);
        }
    }
}
