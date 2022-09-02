import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-f', type=str)
args = parser.parse_args()
name = args.f
times = []
with open(name, 'r') as f:
    for line in f:
        if line.endswith('ms.\n'):
            time = line.split(' ')[-2]
            times.append(int(time))
print(times)
