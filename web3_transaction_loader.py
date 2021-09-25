from web3 import Web3, HTTPProvider
import json
import yaml
from kafka import KafkaProducer
from time import sleep

with open("project.yaml", "r") as stream:
    try:
        configuration = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

producer = KafkaProducer(bootstrap_servers=[configuration['KAFKA_BOOTSTRAP']],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

elementDict= {
			'blockNumber':'blockNumber',
			'chainId':'chainId',
			'from':'from_field', #rename fields with special words
			'to':'to_field',     #rename fields with special words
			'value':'value_field',
			'gas':'gas'
			 }

# return JSON string from transaction
def proceed_raw_transaction(tr_num, transaction): 
	jsonProceeded = {'trNumber': tr_num}
	for key, value in elementDict.items():
		jsonProceeded[value] = transaction[key]
	return jsonProceeded

# write to configaration file
def write_to_configuration():
	with open("project.yaml", "w") as f:
		yaml.dump(configuration, f)

def block_processing():
	w3 = Web3(HTTPProvider('https://kovan.infura.io/v3/' + configuration['INFURA_API_KEY']))
	if w3.isConnected() == True:
		#latest_block_nubmer = w3.eth.blockNumber  #get latest block (was 27337607)
		latest_block_nubmer = configuration['CURRRENT_BLOCK_NUBER'] + 1
		for test_bl_num in range(5): 
			try:
				transactions_in_block = w3.eth.get_block(latest_block_nubmer).transactions
			except Exception as e: 
				print(f"Error: couldn't proceed block {latest_block_nubmer}. \n   With error message: {str(e)}")
				break
			for tr_num in transactions_in_block:
				transaction = w3.eth.get_transaction(tr_num)
				data = proceed_raw_transaction(tr_num.hex(), transaction)
				print(data)
				for i in range(20):
					producer.send("blocks", value=data)
				print("\n"*2)
				sleep(2)
			latest_block_nubmer += 1
			configuration['CURRRENT_BLOCK_NUBER'] = latest_block_nubmer
			sleep(5)
		write_to_configuration()
	else:
		print("Couldn't connect to infura")


if __name__ == '__main__':
    block_processing()