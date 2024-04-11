def binary_to_hex(binary_file):
    with open(binary_file, 'rb') as file:
        binary_data = file.read()

    hex_data = binary_data.encode('hex')

    return hex_data

# Specify the path to your binary file
binary_file_path = 'response.bin'

# Convert binary to hexadecimal
hex_output = binary_to_hex(binary_file_path)

# Print the hexadecimal output
print(hex_output)