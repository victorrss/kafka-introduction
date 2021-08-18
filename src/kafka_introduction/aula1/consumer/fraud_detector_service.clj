(ns kafka-introduction.aula1.consumer.fraud-detector-service
  (:import
    (java.time Duration)
    (java.util Properties)
    (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)))

(defn- cria-as-propriedades-da-conexao-kafka []
  "Define as configurações(Deserializer, Group ID e Server) e devolve uma instância de Properties"
  (doto (Properties.)
    (.putAll {ConsumerConfig/GROUP_ID_CONFIG,                "FraudDetectorService"
              ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringDeserializer"
              ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
              ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        "localhost:9092"})))

(defn cria-consumidor! [topico]
  "Cria o consumidor, análogo ao comando bin/kafka-console-consumer.sh --bootstrap-server HOST:PORTA --topic NOME_DO_TOPICO"
  (with-open [consumidor (KafkaConsumer. (cria-as-propriedades-da-conexao-kafka))]
    ; Habilita a "escuta" do tópico
    (.subscribe consumidor [topico])

    (println "Aguardando novos pedidos...")
    ; Fica em loop infinito para receber as mensagens em realtime
    (while true
      ; Tenta recuperar alguma mensagem
      (let [registros (.poll consumidor (Duration/ofSeconds 1))]
        ; Itera se houver mensagem
        (doseq [registro registros]
          ; Recupera informações da mensagem/registro
          (let [chave (.key registro)
                valor (.value registro)
                particao (.partition registro)
                offset (.offset registro)
                timestamp (.timestamp registro)]

            ; Finge que vai processar o pedido
            (println "------------------------------------------------------")
            (println "Processando novo pedido, checando fraude...")

            ; Printa o que recebeu
            (printf "Pedido: Chave %s | Valor %s | Partição %d | Offset %d | Timestamp %d\n"
                    chave
                    valor
                    particao
                    offset
                    timestamp)

            ; Não faz nada por 5 segundos
            (Thread/sleep 5000)

            ; Finge que processou o pedido
            (println "Pedido processado!")
            (println "------------------------------------------------------")
            (println "Aguardando novos pedidos...")))))))

; Faz a mágica
(cria-consumidor! "STORE_NEW_ORDER")