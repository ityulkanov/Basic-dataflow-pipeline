GENERATE_CMD = java -jar jars/avro-tools-1.11.3.jar compile schema schema/sale.avsc src/main/java

.PHONY: generate-avro
generate-avro:
	$(GENERATE_CMD)
	

copy-files: 
	gsutil cp sample/sales_file.json config/config.json gs://sales_data_32345546/