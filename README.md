###ruta

/home/Voultech/app/ethub-protocol-buff-bcs/
/home/Voultech/app/ethub-protocol-buff-ib/


mvn compile package -Pdistribution



#### INSTALACIÓN
docker rmi 10.0.1.8:5000/ethub-protocol-buff:latest
mvn compile package -P distribution
docker build --no-cache --progress=plain -t ethub-protocol-buff:latest .
docker tag ethub-protocol-buff:latest 10.0.1.8:5000/ethub-protocol-buff:latest
docker push 10.0.1.8:5000/ethub-protocol-buff:latest

### ENTRAR A LA MAQUINA

sudo docker exec -it 7d2441bb751e /bin/sh


docker pull --pull-always 10.0.1.8:5000/ethub-protocol-buff:1.5
docker system prune