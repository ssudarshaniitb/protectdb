
import subprocess

# Command and arguments passed as a list of strings (recommended for safety)
result = subprocess.run(["ifconfig"],capture_output=True, text=True, check=True)
#print(result)
for line in result.stdout:
    print(line)
    if 'inet ' in line:
        parts = line.strip.split()
        print(parts[1])

