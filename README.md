>This is a project for the lecture "Big Data" in third and fourth semester. Course: WWI20DSB  
Contributors:       
- Florian Frey
- Anh Vu
- Frederick Neugebauer
- Olena Lavrikova
- Zabiullah Salehi 

# Use Case: Popular Netflix Movies and Shows


- 
- 
- 


# Architecture: 

![Big Data Platform Architecture ](https://farberg.de/talks/big-data/img/big-data-app.svg)

- Ideen: Das Bild beschreiben, warum wird spark und kafka verwendet, die verschiedenen Datein erklären worfür sie verantwortlich sind

# Implementation

## Data

![Screenshot of the Data](data\Netflix_data.png)


Describe data. Give the source. 

## Content and coding

## Final Application
Show the cast (video demo) if possible, otherwise describe how the application should look like ideally. For example: per text and drawn picture ;)



```

	Titel: 
	Director: 
	timestamp: 1604325221 

```

# Prerequisites

A running Strimzi.io Kafka operator

```bash
helm repo add strimzi http://strimzi.io/charts/
helm install my-kafka-operator strimzi/strimzi-kafka-operator
kubectl apply -f https://farberg.de/talks/big-data/code/helm-kafka-operator/kafka-cluster-def.yaml
```

A running Hadoop cluster with YARN (for checkpointing)

```bash
helm repo add stable https://charts.helm.sh/stable
helm install --namespace=default --set hdfs.dataNode.replicas=1 --set yarn.nodeManager.replicas=1 --set hdfs.webhdfs.enabled=true my-hadoop-cluster stable/hadoop
```

## Deploy

To develop using [Skaffold](https://skaffold.dev/), use `skaffold dev`. 
