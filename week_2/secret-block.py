from prefect.blocks.system import Secret

secret_block = Secret.load("sirun")

# Access the stored secret
data = secret_block.get()
print(data)
