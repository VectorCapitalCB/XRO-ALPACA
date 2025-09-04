package cl.vc.xroalpaca;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.RoundRobinPool;
import cl.vc.xroalpaca.actors.ClientManager;
import cl.vc.xroalpaca.actors.SellSideManager;
import cl.vc.xroalpaca.actors.SendKafkaAndMongo;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufServer;
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
    public static boolean simulador;

    @Getter
    public static boolean ordercancel;

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
    private static String uuid_bloomberg = "";

    @Getter
    private static MessageEventBus messageEventBus = new MessageEventBus();

    public static void main(String[] args) {


        try (FileInputStream fis = new FileInputStream(args[0])) {

            properties = new Properties();
            properties.load(fis);

            if (properties != null) {

                log.info("Starting ETHub v1.3.1");

                String quickFixIniFileSellSide = properties.getProperty("quick-fix-sell-side-file");
                securityExchange = RoutingMessage.SecurityExchangeRouting.valueOf(properties.getProperty("security-exchange"));

                uuid_bloomberg = properties.getProperty("uuid.blommberg");

                system = ActorSystem.create("ethub");

                ordercancel = "true".equals(properties.getProperty("order-cancel"));

                simulador = Boolean.parseBoolean(MainApp.getProperties().getProperty("simulador"));

                clientManager = system.actorOf(new RoundRobinPool(1).props(ClientManager.props()), "client-manager");


                if (!simulador) {
                    sellSideManager = system.actorOf(SellSideManager.props(clientManager, securityExchange, quickFixIniFileSellSide));
                }

                actorKafka = system.actorOf(new RoundRobinPool(1).props(SendKafkaAndMongo.props(properties)), "client-kafka");

                nettyProtobufServer = new NettyProtobufServer(properties.getProperty("core.server.hostname"),
                        clientManager, properties.getProperty("path.logs"), securityExchange.name());
                new Thread(nettyProtobufServer).start();

                log.info("Actors created");

            }

        } catch (Exception exc) {
            log.error("Error al leer parametros:", exc);
            System.exit(0);
        }
    }
}
