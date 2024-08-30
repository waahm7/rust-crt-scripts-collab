from decimal import localcontext

import pandas as pd
import matplotlib.pyplot as plt

# Load the data from a tab-separated CSV file
file_path = 'out.csv'  # Replace with your actual file path
data = pd.read_csv(file_path, sep='\t')  # Use '\t' for tab separator


# Function to create a plot
def create_plot(x, y, xlabel, ylabel, title, data, yticks_range=None):
    plt.figure(figsize=(10, 8))
    for client in data['CLIENT'].unique():
        subset = data[data['CLIENT'] == client]
        plt.plot(subset[x], subset[y], marker='o', label=client, linewidth=2)

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    plt.xticks(range(0, max(data[x]) + 1, 100))  # Set x-axis ticks with a fixed step of 100

    if yticks_range:
        plt.ylim(yticks_range)
        plt.yticks(range(yticks_range[0], yticks_range[1] + 1, 10))
    else:
        plt.yticks(range(0, int(max(data[y])) + 1, 10))

    plt.grid(True)
    plt.legend(loc='upper left', bbox_to_anchor=(-0.05, 1.12), fancybox=True, shadow=True, ncol=1)
    plt.show()

title = """
Concurrently Uploading 8MiB Objects
EC2: c7gn.16xlarge Network= 200Gb/s vCPU=64 Memory=128GiB
"""
# Plot 1: Network Throughput vs. Concurrency
create_plot(
    x='CONCURRENCY',
    y='THROUGHPUT(Gb/s)',
    xlabel='# Concurrent HTTP Requests',
    ylabel='Network Throughput (Gb/s)',
    title='Network Throughput of Different HTTP Clients' + title,
    data=data
)

# Plot 2: CPU Usage vs. Concurrency with y-axis from 0 to 100
create_plot(
    x='CONCURRENCY',
    y='CPU(%)',
    xlabel='# Concurrent HTTP Requests',
    ylabel='CPU Usage (%)',
    title='CPU Usage of Different HTTP Clients' + title,
    data=data,
    yticks_range=(0, 100)  # Set y-axis range from 0 to 100
)

# Plot 3: Memory Usage vs. Concurrency
create_plot(
    x='CONCURRENCY',
    y='MEM(MiB)',
    xlabel='# Concurrent HTTP Requests',
    ylabel='Memory Usage (MiB)',
    title='Memory Usage of Different HTTP Clients' + title,
    data=data
)