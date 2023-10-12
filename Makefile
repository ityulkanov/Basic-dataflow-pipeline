GENERATE_CMD = java -jar jars/avro-tools-1.11.3.jar compile schema schema/sale.avsc src/main/java

.PHONY: generate-avro
generate-avro:
	$(GENERATE_CMD)