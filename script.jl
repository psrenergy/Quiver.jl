stage = 60 # needs 6 bits
scenario  = 1200 # needs 11 bits
block = 720 # needs 10 bits

@time encoded = stage + (scenario << 6) + (block << (11 + 6))

decoded = [encoded & 0x3F, (encoded >> 6) & 0x7FF, (encoded >> (11 + 6)) & 0x3FF]