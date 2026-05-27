[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/BK9AX0KL)
# RabbitMQ-Example
Example based on Tanenbaum &amp; van Steen (2025)

# Steps to run:

## Ports to open on the firewall (security group on AWS):
```
5671-5672
```

## Install the RabbitMQ broker on a server machine:
You may use the provided script for installation (install_rabbitmq.sh)
```
sudo install_rabbitmq.sh
```
*Note:* Make sure the file is executable (chmod 770 install_rabbitmq.sh)

See installation and configuration details on: https://www.rabbitmq.com/docs/install-debian#apt-quick-start (although the defaults should work just fine for our purposes).

### Once installed, put the broker to run:
```
sudo systemctl start rabbitmq-server
```
### Then create a new RabbitMQ user and password:
```
sudo rabbitmqctl add_user myuser abc123
```
### Now create a vhost in the RabbitMQ server (a vhost is like a container for message queues)?
```
sudo rabbitmqctl add_vhost my_vhost
```
### And give the new user the required permisssions to access the vhost:
```
sudo rabbitmqctl set_permissions -p my_vhost myuser ".*" ".*" ".*"
```

## Finally, install the RabbitMQ python client on the machines where producers and consumers will run:
```
pip install rabbitpy
```

*Note:* Make sure the IP address of the RabbitMQ server is correctly set in const.py

---

# Solucao da tarefa (minimalista, sem apagar arquivos antigos)

Esta implementacao da tarefa esta no arquivo `app_task.py`.

## O que foi implementado

- Varias filas distintas: `temperature.raw.a`, `temperature.raw.b`, `temperature.processed`, `temperature.alerts`
- Mais de um produtor: `producer-a` e `producer-b`
- Varios consumidores especificos:
	- `processor-a` consome `temperature.raw.a`
	- `processor-b` consome `temperature.raw.b`
	- `storage` consome `temperature.processed` e salva no SQLite
	- `alerts` consome `temperature.alerts` e simula notificacao
- Mensagens de aplicacao (temperatura), nao mensagens genericas

## Execucao minima

1) Subir RabbitMQ:

```bash
docker compose up -d
```

2) Instalar dependencia:

```bash
pip install -r requirements.txt
```

3) Rodar em terminais separados:

```bash
python app_task.py storage
python app_task.py alerts
python app_task.py processor-a
python app_task.py processor-b
python app_task.py producer-a
python app_task.py producer-b
```

4) Consultar historico salvo:

```bash
python app_task.py history --limit 10
```

## Comparacao RabbitMQ/AMQP x Kafka (aplicacao implementada)

1. RabbitMQ/AMQP foi mais direto para separar responsabilidades em filas e tratar cada tipo de tarefa com consumidores especificos.
2. Kafka e mais forte para pipeline de eventos com historico longo e reprocessamento por offset.
3. Para esta solucao minimalista de monitoramento e alertas, RabbitMQ ficou mais simples de desenvolver e operar.
4. Para cenarios com volume muito alto e foco em streaming/analytics, Kafka tende a escalar melhor.
