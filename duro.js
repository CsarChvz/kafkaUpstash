const { Kafka } = require("kafkajs");
const axios = require("axios");

const kafka = new Kafka({
  brokers: ["engaging-swan-10732-us1-kafka.upstash.io:9092"],
  sasl: {
    mechanism: "scram-sha-256",
    username: "ZW5nYWdpbmctc3dhbi0xMDczMiR4sCuIBVuISYn-VLSLYlosq_1_SK_gQQrC0Uc",
    password: "ZjBjYjA4ODctZTBiMC00MmQ4LTk3NDItNTQwNWU2Y2QwMzQ4",
  },
  ssl: true,
});

const topicName = "expedientes";
const groupId = "expedientes-busqueda";

async function fetchLag() {
  const admin = kafka.admin();
  await admin.connect();

  // Obtener la última offset de cada partición
  const topicOffsets = await admin.fetchTopicOffsets(topicName);
  // Obtener la última offset consumida por el grupo de consumidores
  const consumerOffsets = await admin.fetchOffsets({
    groupId: groupId,
    topic: topicName,
  });

  await admin.disconnect();

  consumerOffsets.forEach((consumerOffset) => {
    consumerOffset.partitions.forEach((partition) => {
      const topicOffset = topicOffsets.find(
        (topicOffset) => topicOffset.partition === partition.partition
      );

      // Calcular el lag
      const lag = topicOffset.offset - partition.offset;
      if (lag > 0) {
        console.log("pedejo");
      }
      console.log(
        `Partición: ${partition.partition}, Offset: ${partition.offset}, Lag: ${lag}`
      );
    });
  });
}

fetchLag();
